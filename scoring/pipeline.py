"""Local/staged-Parquet graph preparation and label-free frozen inference.

Run from the repository root with python -m scoring.pipeline --help.
S3 inputs must first be staged locally; outputs are published only after _SUCCESS.
"""
import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from .vendor.feature_engineering import chronological_structure, node_snapshot
from .vendor.graph_sampling import IncomingSampler

VENDOR = Path(__file__).parent / 'vendor'
ORIGIN = pd.Timestamp('2022-09-01')


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2), encoding='utf-8')


def fresh(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=False)
    return path


def bundle(source, base, out):
    source, base, out = Path(source), Path(base), fresh(out)
    for name in ('selected_model.pt', 'selected_validation.json', 'features_manifest.json',
                 'config.json', 'versions.json'):
        shutil.copyfile(source / name, out / name)
    shutil.copyfile(base / 'nodes.csv', out / 'nodes.csv')
    config, fitted, selected = load_bundle(out, verify_manifest=False)
    if read(base / 'manifest.json')['source_sha256'] != selected['source_sha256']:
        raise ValueError('Saved node mapping belongs to another training source')
    write(out / 'bundle.json', {'model_version': selected['model_version'],
          'training_source_sha256': selected['source_sha256'],
          'files': {p.name: sha(p) for p in out.iterdir() if p.is_file()}})
    (out / '_SUCCESS').touch()


def load_bundle(path, verify_manifest=True):
    path = Path(path)
    if verify_manifest:
        if not (path / '_SUCCESS').exists():
            raise ValueError('Incomplete model bundle')
        for name, digest in read(path / 'bundle.json')['files'].items():
            if sha(path / name) != digest:
                raise ValueError(f'Bundle checksum mismatch: {name}')
    config = read(path / 'config.json')
    fitted = read(path / 'features_manifest.json')
    selected = read(path / 'selected_validation.json')
    checks = [(sha(path / 'selected_model.pt')[:16], selected['model_version']),
              (sha(path / 'features_manifest.json'), selected['feature_manifest_sha256']),
              (sha(VENDOR / 'feature_engineering.py'), fitted['code_sha256']),
              (sha(VENDOR / 'models.py'), config['model_source_sha256']),
              (fitted['source_sha256'], selected['source_sha256']),
              (config['source_sha256'], selected['source_sha256'])]
    if any(actual != expected for actual, expected in checks):
        raise ValueError('Model or feature implementation differs from frozen artifacts')
    if (config['edge_dim'], config['node_dim']) != (35, 30):
        raise ValueError('Only the enriched 35-edge/30-node model is supported')
    return config, fitted, selected


def frozen_features(edges, times, amounts, currencies, formats, banks, fitted, cutoff,
                    x=None, encoded=False):
    """Apply training-fitted transforms; the caller has already filtered visibility."""
    if not len(edges) or times.min() < 0 or times.max() >= cutoff:
        raise ValueError('Context must be nonempty and within origin/cutoff')
    cur_map, fmt_map = (fitted['categorical_dictionaries'][k] for k in ('currency', 'format'))
    nc, nf = len(cur_map) + 1, len(fmt_map) + 1
    offset = 7 + nc
    if x is None:
        x = np.empty((len(edges), 35), dtype=np.float32)
    unknown = {'currency': 0, 'payment_format': 0}
    for p in range(0, len(edges), 200000):
        q = min(p + 200000, len(edges))
        cur = currencies[p:q] if encoded else np.array([cur_map.get(v, 0) for v in currencies[p:q]])
        fmt = formats[p:q] if encoded else np.array([fmt_map.get(v, 0) for v in formats[p:q]])
        unknown['currency'] += int((cur == 0).sum())
        unknown['payment_format'] += int((fmt == 0).sum())
        t, e = times[p:q], edges[p:q]
        hour = t % 86400 / 86400 * 2 * np.pi
        weekday = (t // 86400 + 3) % 7 / 7 * 2 * np.pi
        x[p:q, :7] = np.column_stack([
            (np.log1p(amounts[p:q]) - np.asarray(fitted['currency_log_mean'])[cur]) /
            np.asarray(fitted['currency_log_std'])[cur],
            np.sin(hour), np.cos(hour), np.sin(weekday), np.cos(weekday),
            e[:, 0] == e[:, 1], banks[e[:, 0]] != banks[e[:, 1]]])
        x[p:q, 7:offset] = np.eye(nc, dtype=np.float32)[cur]
        x[p:q, offset:offset + nf] = np.eye(nf, dtype=np.float32)[fmt]
    print(json.dumps({'stage': 'graph-structure', 'rows': len(edges)}), flush=True)
    structure = chronological_structure(edges, times, len(banks))
    for p in range(0, len(edges), 200000):
        x[p:p+200000, -4:] = (structure[p:p+200000] - fitted['structure_mean']) / fitted['structure_std']
    del structure
    print(json.dumps({'stage': 'node-snapshot', 'nodes': len(banks)}), flush=True)
    nodes, _ = node_snapshot(edges, times, x, len(banks), cutoff, offset, nf)
    for p in range(0, len(nodes), 200000):
        nodes[p:p+200000] = ((nodes[p:p+200000] - fitted['node_scaling']['mean']) /
                            fitted['node_scaling']['std']).astype(np.float32)
    if any(not np.isfinite(x[p:p+200000]).all() for p in range(0, len(x), 200000)) or not np.isfinite(nodes).all():
        raise ValueError('Nonfinite model inputs')
    return x, nodes, unknown


def prepare(curated, model, out, start, cutoff, bank=None):
    from .staging import stage
    if Path(out).exists():
        raise FileExistsError(out)
    config, fitted, selected = load_bundle(model)
    start, cutoff = pd.Timestamp(start), pd.Timestamp(cutoff)
    if start.tzinfo or cutoff.tzinfo or not ORIGIN <= start < cutoff:
        raise ValueError('Use timezone-unspecified origin <= start < cutoff')
    curated = Path(curated)
    if not (curated / '_SUCCESS').exists():
        raise ValueError('Curated build is incomplete')
    seconds = int((cutoff - ORIGIN).total_seconds())
    out = fresh(out)
    print(json.dumps({'stage': 'staging', 'cutoff': str(cutoff)}), flush=True)
    with tempfile.TemporaryDirectory(prefix='staging-', dir=out) as scratch:
        arrays, banks, source = stage(curated, model, out, scratch, ORIGIN, cutoff, bank, fitted)
        edges, times = arrays['edge'], arrays['time']
        edge_x = np.lib.format.open_memmap(out / 'edge_features.npy', mode='w+',
                                           dtype=np.float32, shape=(len(edges), 35))
        try:
            edge_x, node_x, unknown = frozen_features(edges, times, arrays['amount'],
                arrays['currency'], arrays['format'], banks, fitted, seconds, x=edge_x, encoded=True)
            edge_x.flush()
        finally:
            # Windows requires scratch memory maps to be closed before removing files.
            for name in ('amount', 'currency', 'format'):
                arrays.pop(name)._mmap.close()
    incoming = np.argsort(edges[:, 1], kind='stable')
    ptr = np.r_[0, np.cumsum(np.bincount(edges[:, 1], minlength=len(banks)), dtype=np.int64)]
    for name, values in dict(incoming=incoming, ptr=ptr, node_features=node_x).items():
        np.save(out / f'{name}.npy', values)
    manifest = dict(model_version=selected['model_version'], source_sha256=source,
                    training_source_sha256=selected['source_sha256'],
                    feature_manifest_sha256=sha(Path(model) / 'features_manifest.json'),
                    start=str(start), cutoff=str(cutoff), cutoff_exclusive_seconds=seconds,
                    score_start=int(np.searchsorted(times, int((start - ORIGIN).total_seconds()))),
                    score_end=len(edges), context_rows=len(edges), num_nodes=len(banks),
                    visibility=bank, unknown_categories=unknown, labels_used=False)
    write(out / 'context_manifest.json', manifest)
    (out / '_SUCCESS').touch()
    return manifest


def score_arrays(base, context, model, out, lo, hi, device='cpu'):
    import torch
    from .vendor.models import GINe
    config, _, selected = load_bundle(model)
    base, context, out = Path(base), Path(context), Path(out)
    arrays = {k: np.load(base / f'{k}.npy', mmap_mode='r') for k in ('edge', 'incoming', 'ptr')}
    x = np.load(context / 'edge_features.npy', mmap_mode='r')
    nodes = np.load(context / 'node_features.npy', mmap_mode='r')
    if not 0 <= lo <= hi <= len(x) or hi > len(arrays['edge']):
        raise ValueError('Score range exceeds visible context')
    sampler = IncomingSampler(arrays['edge'], arrays['incoming'], arrays['ptr'], config['neighbours'])
    torch.set_num_threads(4)
    net = GINe(num_features=30, num_gnn_layers=config['layers'], n_hidden=config['hidden'],
               edge_dim=35, edge_updates=config['edge_updates'], final_dropout=.1).to(device)
    net.load_state_dict(torch.load(Path(model) / 'selected_model.pt', map_location=device, weights_only=True), strict=True)
    net.eval()
    scores = np.lib.format.open_memmap(out / 'scores.npy', mode='w+', dtype=np.float32, shape=(hi-lo,))
    rng = np.random.default_rng(config['seed'])
    with torch.inference_mode():
        for p in range(lo, hi, config['batch_size']):
            seeds = np.arange(p, min(p + config['batch_size'], hi))
            ids, edge_ids, local, positions = sampler.sample(seeds, hi, rng)
            logits = net(torch.tensor(nodes[ids], device=device),
                         torch.tensor(local, dtype=torch.long, device=device),
                         torch.tensor(x[edge_ids], device=device))[positions]
            values = logits.softmax(-1)[:, 1].cpu().numpy()
            if not np.isfinite(values).all():
                raise ValueError('Nonfinite scores')
            scores[p-lo:p-lo+len(values)] = values
            if (p-lo) // config['batch_size'] % 250 == 0:
                print(json.dumps({'stage': 'inference', 'completed': p-lo+len(values),
                                  'total': hi-lo}), flush=True)
    scores.flush()
    return selected


def infer(context, model, out, device='cpu'):
    context = Path(context)
    if not (context / '_SUCCESS').exists():
        raise ValueError('Incomplete graph context')
    manifest = read(context / 'context_manifest.json')
    _, _, selected = load_bundle(model)
    if (manifest['model_version'] != selected['model_version'] or
        manifest['feature_manifest_sha256'] != selected['feature_manifest_sha256']):
        raise ValueError('Context and selected model differ')
    out = fresh(out)
    score_arrays(context, context, model, out, manifest['score_start'], manifest['score_end'], device)
    write(out / 'inference.json', dict(manifest, scores_sha256=sha(out / 'scores.npy'),
                                      device=device, calibrated=False, labels_used=False))
    (out / '_SUCCESS').touch()


def rank_arrays(edges, rows, scores, count, k, eligible=None):
    if len(edges) != len(rows) or len(edges) != len(scores) or not np.isfinite(scores).all():
        raise ValueError('Scores and transfer identities must align')
    account_scores = np.full(count, -np.inf, dtype=np.float32)
    for side in (0, 1):
        np.maximum.at(account_scores, edges[:, side], scores)
    candidates = np.flatnonzero(np.isfinite(account_scores))
    if eligible is not None:
        candidates = np.intersect1d(candidates, eligible)
    top = candidates[np.lexsort((candidates, -account_scores[candidates]))][:k]
    evidence = np.full(count, np.iinfo(np.int64).max, dtype=np.int64)
    for side in (0, 1):
        ids = edges[:, side]
        match = scores == account_scores[ids]
        np.minimum.at(evidence, ids[match], rows[match])
    return top, account_scores[top], evidence[top]


def rank(context, inference, out, k, daily=False, bank=None):
    context, inference = Path(context), Path(inference)
    m = read(context / 'context_manifest.json')
    run = read(inference / 'inference.json')
    if not (inference / '_SUCCESS').exists() or sha(inference / 'scores.npy') != run['scores_sha256']:
        raise ValueError('Incomplete or modified scores')
    if any(run[key] != value for key, value in m.items()):
        raise ValueError('Scores belong to a different context')
    if k <= 0:
        raise ValueError('Review budget must be positive')
    if m['visibility'] is not None:
        if bank is not None and bank != m['visibility']:
            raise ValueError('Review cohort bank differs from graph visibility')
        bank = m['visibility']
    if daily and (pd.Timestamp(m['start']) != pd.Timestamp(m['start']).normalize() or
                  pd.Timestamp(m['cutoff']) - pd.Timestamp(m['start']) != pd.Timedelta(days=1)):
        raise ValueError('Daily ranking requires one midnight-to-midnight scoring window')
    sl = slice(m['score_start'], m['score_end'])
    edges = np.load(context / 'edge.npy', mmap_mode='r')[sl]
    rows = np.load(context / 'row.npy', mmap_mode='r')[sl]
    scores = np.load(inference / 'scores.npy', mmap_mode='r')
    identities = pd.read_csv(context / 'nodes.csv', dtype={'bank_id': str, 'account_id': str})
    eligible = np.unique(edges[edges[:, 0] != edges[:, 1]]) if daily else np.unique(edges)
    if bank is not None:
        eligible = np.intersect1d(eligible, identities.loc[identities.bank_id == bank, 'node_id'])
    top, values, evidence = rank_arrays(edges, rows, scores, len(identities), k, eligible)
    result = identities.iloc[top].copy()
    result['score'], result['rank'] = values, np.arange(1, len(top)+1)
    result['supporting_transfer_id'] = [m['source_sha256'] + ':' + str(r) for r in evidence]
    result['model_version'], result['window_start'], result['cutoff'] = m['model_version'], m['start'], m['cutoff']
    out = fresh(out)
    result.to_csv(out / 'accounts.csv', index=False)
    # Publish transfer scores without opening a label table. Stream batches so the
    # export does not add another full-size string table to the graph's memory use.
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([('transfer_id', pa.string()), ('risk_score', pa.float32()),
                        ('model_version', pa.string()), ('cutoff', pa.string())])
    with pq.ParquetWriter(out / 'transfers.parquet', schema) as writer:
        for offset in range(0, len(rows), 200000):
            end = min(offset + 200000, len(rows))
            writer.write_table(pa.table({
                'transfer_id': [m['source_sha256'] + ':' + str(r) for r in rows[offset:end]],
                'risk_score': scores[offset:end], 'model_version': [m['model_version']]*(end-offset),
                'cutoff': [m['cutoff']]*(end-offset)}, schema=schema))
    write(out / 'ranking.json', dict(m, requested_k=k, returned=len(top),
          eligible_accounts=len(eligible), daily=daily, cohort_bank=bank,
          aggregation='maximum incident transfer score, including self-transfers',
          eligibility='non-self transfer that day' if daily else 'any transfer in window'))
    (out / '_SUCCESS').touch()


def evaluate(context, ranking, labels, out):
    """Optional ground-truth join, kept outside preparation and inference."""
    import pyarrow.dataset as ds
    context, ranking = Path(context), Path(ranking)
    m = read(context / 'context_manifest.json')
    policy = read(ranking / 'ranking.json')
    if not (ranking / '_SUCCESS').exists() or any(policy[key] != value for key, value in m.items()):
        raise ValueError('Ranking and context differ or are incomplete')
    sl = slice(m['score_start'], m['score_end'])
    edges = np.load(context / 'edge.npy', mmap_mode='r')[sl]
    rows = np.load(context / 'row.npy', mmap_mode='r')[sl]
    truth = ds.dataset(labels, format='parquet').to_table(
        columns=['source_row_id', 'laundering_mark'],
        filter=((ds.field('source_sha256') == m['source_sha256']) &
                (ds.field('source_row_id') >= int(rows.min())) &
                (ds.field('source_row_id') <= int(rows.max())))).to_pandas() if len(rows) else pd.DataFrame(
                    {'source_row_id': pd.Series(dtype='int64'), 'laundering_mark': pd.Series(dtype='int64')})
    if truth.source_row_id.duplicated().any():
        raise ValueError('Duplicate ground-truth IDs')
    marks = truth.set_index('source_row_id').laundering_mark.reindex(rows)
    if marks.isna().any() or not marks.isin([0, 1]).all():
        raise ValueError('Missing or invalid ground truth for scored transfers')
    eligible = np.unique(edges[edges[:, 0] != edges[:, 1]]) if policy['daily'] else np.unique(edges)
    if policy['cohort_bank'] is not None:
        identities = pd.read_csv(context / 'nodes.csv', dtype={'bank_id': str, 'account_id': str})
        eligible = np.intersect1d(eligible, identities.loc[identities.bank_id == policy['cohort_bank'], 'node_id'])
    positives = np.intersect1d(eligible, np.unique(edges[marks.to_numpy() == 1]))
    reviewed = pd.read_csv(ranking / 'accounts.csv').node_id.to_numpy()
    tp = len(np.intersect1d(reviewed, positives))
    result = dict(model_version=m['model_version'], start=m['start'], cutoff=m['cutoff'],
                  daily=policy['daily'], k=policy['requested_k'], reviewed=len(reviewed),
                  eligible=len(eligible), positive_accounts=len(positives), true_positives=tp,
                  precision_at_k=tp/len(reviewed) if len(reviewed) else None,
                  recall_at_k=tp/len(positives) if len(positives) else None)
    out = fresh(out)
    write(out/'evaluation.json', result)
    (out/'_SUCCESS').touch()
    return result


def replay(base, context, model, expected, out, device):
    base, context, expected = Path(base), Path(context), Path(expected)
    _, _, selected = load_bundle(model)
    manifest = read(context / 'context_manifest.json')
    source = read(base / 'manifest.json')
    if manifest['source_sha256'] != selected['source_sha256'] or source['source_sha256'] != selected['source_sha256']:
        raise ValueError('Replay requires the original training source')
    if manifest['feature_manifest_sha256'] != selected['feature_manifest_sha256']:
        raise ValueError('Replay feature specification mismatch')
    lo, hi = manifest['score_start'], manifest['score_end']
    out = fresh(out)
    score_arrays(base, context, model, out, lo, hi, device)
    actual, reference = (np.load(p, mmap_mode='r') for p in (out/'scores.npy', expected))
    np.testing.assert_allclose(actual, reference, rtol=1e-5, atol=1e-6)
    edge = np.load(base / 'edge.npy', mmap_mode='r')[lo:hi]
    rows = np.load(base / 'row.npy', mmap_mode='r')[lo:hi]
    a = rank_arrays(edge, rows, actual, source['num_nodes'], selected['k'])
    b = rank_arrays(edge, rows, reference, source['num_nodes'], selected['k'])
    np.testing.assert_array_equal(a[0], b[0])
    np.testing.assert_array_equal(a[2], b[2])
    write(out / 'replay.json', {'status': 'passed', 'rows': hi-lo,
          'max_absolute_difference': float(np.max(np.abs(actual-reference))),
          'top_accounts_and_evidence_match': True, 'model_version': selected['model_version']})
    (out / '_SUCCESS').touch()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    for name, required in [('bundle', ['source', 'base', 'out']),
                           ('prepare', ['curated', 'model', 'out', 'start', 'cutoff']),
                           ('infer', ['context', 'model', 'out']),
                           ('rank', ['context', 'inference', 'out', 'k']),
                           ('evaluate', ['context', 'ranking', 'labels', 'out']),
                           ('replay', ['base', 'context', 'model', 'expected', 'out'])]:
        cmd = sub.add_parser(name)
        for arg in required:
            cmd.add_argument('--'+arg, required=True, type=int if arg == 'k' else str)
        if name in ('infer', 'replay'):
            cmd.add_argument('--device', choices=['cpu', 'cuda'], default='cpu')
        if name in ('prepare', 'rank'):
            cmd.add_argument('--bank')
        if name == 'rank':
            cmd.add_argument('--daily', action='store_true')
    args = vars(p.parse_args())
    command = args.pop('command')
    globals()[command](**args)


if __name__ == '__main__':
    main()
