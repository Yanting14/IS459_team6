"""Leakage-aware richer inputs for the IBM GIN model.

Features use transaction attributes and observed graph structure only. Labels are
not opened here. Categorical dictionaries and continuous scaling fit on training
data. Causal counterparty order and elapsed times depend only on earlier edges.
Account summaries are recomputed independently for each graph cutoff.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2), encoding='utf-8')


def chronological_structure(edges, times, num_nodes):
    """Return log counterparty ports and previous-event gaps for each edge.

    Port numbers are assigned on first contact, with original edge order breaking
    same-timestamp ties. Appending later edges cannot change earlier features.
    """
    pairs = edges[:, 0].astype(np.int64) * num_nodes + edges[:, 1]
    keys, first, inverse = np.unique(pairs, return_index=True, return_inverse=True)
    result = np.zeros((len(edges), 4), dtype=np.float32)
    for side in (0, 1):
        endpoint = keys // num_nodes if side == 0 else keys % num_nodes
        order = np.lexsort((first, endpoint))
        sorted_nodes = endpoint[order]
        starts = np.r_[True, sorted_nodes[1:] != sorted_nodes[:-1]]
        group_start = np.maximum.accumulate(np.where(starts, np.arange(len(order)), 0))
        port = np.empty(len(keys), dtype=np.int32)
        port[order] = np.arange(len(order)) - group_start
        result[:, side] = np.log1p(port[inverse])

        order = np.argsort(edges[:, side], kind='stable')
        same = edges[order[1:], side] == edges[order[:-1], side]
        gaps = np.r_[0, np.where(same, times[order[1:]] - times[order[:-1]], 0)]
        if np.any(gaps < 0):
            raise ValueError('Edges must be chronological before computing gaps')
        result[order, 2 + side] = np.log1p(gaps)
    return result


def node_snapshot(edges, times, edge_features, num_nodes, cutoff_seconds, format_offset, format_size):
    """Summarise only the explicitly supplied, cutoff-filtered graph."""
    if len(times) and times.max() >= cutoff_seconds:
        raise ValueError('Future edge supplied to account context')
    counts = np.column_stack([np.bincount(edges[:, s], minlength=num_nodes) for s in (0, 1)])
    total = np.zeros((num_nodes, 2)); square = np.zeros_like(total)
    first = np.full(num_nodes, cutoff_seconds); last = np.zeros(num_nodes)
    payment = np.zeros((num_nodes, 2, format_size), dtype=np.float32)
    self_count = np.zeros(num_nodes)
    for p in range(0, len(edges), 200000):
        q = min(p + 200000, len(edges)); e = edges[p:q]
        amount = edge_features[p:q, 0]
        fmt = edge_features[p:q, format_offset:format_offset + format_size].argmax(1)
        for s in (0, 1):
            total[:, s] += np.bincount(e[:, s], weights=amount, minlength=num_nodes)
            square[:, s] += np.bincount(e[:, s], weights=amount ** 2, minlength=num_nodes)
            np.minimum.at(first, e[:, s], times[p:q]); np.maximum.at(last, e[:, s], times[p:q])
            np.add.at(payment[:, s, :], (e[:, s], fmt), 1)
        self_mask = e[:, 0] == e[:, 1]
        np.add.at(self_count, e[self_mask, 0], 1)
    unique_pairs = np.unique(edges[:, 0].astype(np.int64) * num_nodes + edges[:, 1])
    unique = np.column_stack([np.bincount(unique_pairs // num_nodes, minlength=num_nodes),
                              np.bincount(unique_pairs % num_nodes, minlength=num_nodes)])
    mean = total / np.maximum(counts, 1)
    std = np.sqrt(np.maximum(square / np.maximum(counts, 1) - mean ** 2, 0))
    days = max(cutoff_seconds / 86400, 1 / 1440)
    proportions = payment / np.maximum(counts[:, :, None], 1)
    x = np.column_stack([
        np.log1p(counts / days), np.log1p(unique / days),
        unique / np.maximum(counts, 1), mean, std,
        proportions.reshape(num_nodes, -1),
        np.log1p(np.maximum(cutoff_seconds - first, 0) / 3600),
        np.log1p(np.maximum(cutoff_seconds - last, 0) / 3600),
        self_count / np.maximum(counts.sum(1), 1),
        counts[:, 0] / np.maximum(counts.sum(1), 1),
    ]).astype(np.float32)
    active = counts.sum(1) > 0
    x[~active] = 0
    return x, active


def prepare(base, out):
    base, out = Path(base), Path(out)
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'features_manifest.json').exists():
        raise ValueError('Feature preparation already complete; use a new directory')
    manifest = json.loads((base / 'manifest.json').read_text())
    old = json.loads((base / 'preprocessing.json').read_text())
    stop = manifest['splits']['validation']['end']
    train_end = manifest['splits']['train']['end']
    edge = np.load(base / 'edge.npy', mmap_mode='r')[:stop]
    times = np.load(base / 'time.npy', mmap_mode='r')[:stop]
    rows = np.load(base / 'row.npy', mmap_mode='r')[:stop]
    raw = np.load(base / 'raw.npy', mmap_mode='r')
    legacy = np.load(base / 'features.npy', mmap_mode='r')
    n_currency = len(old['categories']['currency']) + 1
    n_format = len(old['categories']['format']) + 1
    format_offset = 7 + n_currency
    structure_offset = format_offset + n_format
    width = structure_offset + 4
    x = np.lib.format.open_memmap(out / 'edge_features.npy', mode='w+',
                                  dtype=np.float32, shape=(stop, width))
    currency_count = np.zeros(n_currency); total = np.zeros(n_currency); square = np.zeros(n_currency)
    for p in range(0, train_end, 200000):
        q = min(train_end, p + 200000)
        codes = np.rint(legacy[p:q, 2] * old['std'][2] + old['mean'][2]).astype(int)
        amount = np.log1p(raw['amount'][rows[p:q]])
        currency_count += np.bincount(codes, minlength=n_currency)
        total += np.bincount(codes, weights=amount, minlength=n_currency)
        square += np.bincount(codes, weights=amount ** 2, minlength=n_currency)
    mean = total / np.maximum(currency_count, 1)
    std = np.sqrt(np.maximum(square / np.maximum(currency_count, 1) - mean ** 2, 0))
    overall_mean = total.sum() / currency_count.sum()
    overall_std = np.sqrt(square.sum() / currency_count.sum() - overall_mean ** 2)
    mean[currency_count == 0] = overall_mean; std[currency_count == 0] = overall_std
    std = np.maximum(std, 1e-6)
    bank_ids = pd.read_csv(base / 'nodes.csv', dtype={'bank_id': str}, usecols=['bank_id'])
    bank_codes = pd.factorize(bank_ids.bank_id, sort=True)[0]
    for p in range(0, stop, 200000):
        q = min(stop, p + 200000)
        cur = np.rint(legacy[p:q, 2] * old['std'][2] + old['mean'][2]).astype(int)
        fmt = np.rint(legacy[p:q, 3] * old['std'][3] + old['mean'][3]).astype(int)
        amount = np.log1p(raw['amount'][rows[p:q]])
        hour = times[p:q] % 86400 / 86400 * 2 * np.pi
        weekday = (times[p:q] // 86400 + 3) % 7 / 7 * 2 * np.pi
        x[p:q, :7] = np.column_stack([
            (amount - mean[cur]) / std[cur], np.sin(hour), np.cos(hour),
            np.sin(weekday), np.cos(weekday), edge[p:q, 0] == edge[p:q, 1],
            bank_codes[edge[p:q, 0]] != bank_codes[edge[p:q, 1]],
        ])
        x[p:q, 7:format_offset] = np.eye(n_currency, dtype=np.float32)[cur]
        x[p:q, format_offset:structure_offset] = np.eye(n_format, dtype=np.float32)[fmt]
    print('transaction attributes prepared', x.shape, flush=True)
    structure = chronological_structure(edge, times, manifest['num_nodes'])
    structure_mean = structure[:train_end].mean(0, dtype=np.float64)
    structure_std = np.maximum(structure[:train_end].std(0, dtype=np.float64), 1e-6)
    for p in range(0, stop, 200000):
        x[p:p + 200000, structure_offset:] = (structure[p:p + 200000] - structure_mean) / structure_std
    del structure
    x.flush()
    node_stats = None
    node_names = (['log_out_per_day', 'log_in_per_day', 'log_unique_out_per_day', 'log_unique_in_per_day',
                   'unique_out_fraction', 'unique_in_fraction', 'mean_relative_amount_out', 'mean_relative_amount_in',
                   'std_relative_amount_out', 'std_relative_amount_in'] +
                  [f'format_{s}_{i}_fraction' for s in ('out', 'in') for i in range(n_format)] +
                  ['log_observed_age_hours', 'log_recency_hours', 'self_fraction', 'out_fraction'])
    for split in ('train', 'validation'):
        end = manifest['splits'][split]['end']
        cutoff = int(times[end - 1]) + 60
        node_x, active = node_snapshot(edge[:end], times[:end], x[:end], manifest['num_nodes'], cutoff, format_offset, n_format)
        if split == 'train':
            node_stats = dict(mean=node_x[active].mean(0, dtype=np.float64).tolist(),
                              std=np.maximum(node_x[active].std(0, dtype=np.float64), 1e-6).tolist())
        node_x = ((node_x - node_stats['mean']) / node_stats['std']).astype(np.float32)
        if not np.isfinite(node_x).all(): raise ValueError('Nonfinite account features')
        np.save(out / f'{split}_nodes.npy', node_x)
        print('account snapshot prepared', split, node_x.shape, 'cutoff', cutoff, flush=True)
    feature_names = (['currency_relative_log_amount', 'hour_sin', 'hour_cos', 'weekday_sin', 'weekday_cos',
                      'self_transfer', 'cross_bank'] + [f'currency_{i}' for i in range(n_currency)] +
                     [f'format_{i}' for i in range(n_format)] + ['out_port', 'in_port', 'out_gap', 'in_gap'])
    write_json(out / 'features_manifest.json', dict(
        base=str(base.resolve()), source_sha256=manifest['source_sha256'], rows=stop,
        train_rows=train_end, edge_features=feature_names, node_features=node_names,
        categorical_dictionaries=old['categories'], unknown_code=0,
        currency_log_mean=mean.tolist(), currency_log_std=std.tolist(),
        structure_mean=structure_mean.tolist(), structure_std=structure_std.tolist(),
        node_scaling=node_stats, visibility='all banks / shared',
        fitting='train only', labels_used=False, context='snapshot filtered before each cutoff',
        prepared_splits=['train', 'validation'],
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='miscellaneous/full_run')
    parser.add_argument('--out', default='miscellaneous/gin_v2')
    args = parser.parse_args()
    prepare(args.base, args.out)
