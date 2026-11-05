import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scoring import pipeline as p


def fitted():
    return dict(categorical_dictionaries={'currency': {str(i): i for i in range(1, 16)},
                                         'format': {str(i): i for i in range(1, 8)}},
                currency_log_mean=[0]*16, currency_log_std=[1]*16,
                structure_mean=[0]*4, structure_std=[1]*4,
                node_scaling={'mean': [0]*30, 'std': [1]*30})


def test_bundle_rejects_changed_weights_and_wrong_source_mapping(tmp_path):
    source, base = tmp_path/'source', tmp_path/'base'
    source.mkdir()
    base.mkdir()
    (source/'selected_model.pt').write_bytes(b'checkpoint fixture')
    p.write(source/'features_manifest.json', {'source_sha256': 'training-source',
            'code_sha256': p.sha(p.VENDOR/'feature_engineering.py')})
    p.write(source/'config.json', {'source_sha256': 'training-source', 'edge_dim': 35,
            'node_dim': 30, 'model_source_sha256': p.sha(p.VENDOR/'models.py')})
    p.write(source/'selected_validation.json', {'source_sha256': 'training-source',
            'model_version': p.sha(source/'selected_model.pt')[:16],
            'feature_manifest_sha256': p.sha(source/'features_manifest.json')})
    p.write(source/'versions.json', {})
    p.write(base/'manifest.json', {'source_sha256': 'training-source'})
    (base/'nodes.csv').write_text('node_id,bank_id,account_id\n0,010,001\n')
    out = tmp_path/'bundle'
    p.bundle(source, base, out)
    p.load_bundle(out)
    (out/'selected_model.pt').write_bytes(b'changed checkpoint')
    with pytest.raises(ValueError, match='checksum'):
        p.load_bundle(out)
    p.write(base/'manifest.json', {'source_sha256': 'other-source'})
    with pytest.raises(ValueError, match='another training source'):
        p.bundle(source, base, tmp_path/'wrong-bundle')
    assert not (tmp_path/'wrong-bundle'/'_SUCCESS').exists()


def test_frozen_features_preserve_parallel_edges_self_and_unknown_categories():
    edges = np.array([[0, 1], [0, 1], [1, 1]])
    x, nodes, unknown = p.frozen_features(edges, np.array([0, 60, 120]),
        np.array([9., 99., 999.]), ['1', 'new', '1'], ['1', '1', 'new'],
        np.array(['010', '1']), fitted(), 180)
    assert x.shape == (3, 35) and nodes.shape == (2, 30)
    np.testing.assert_allclose(x[:, 0], np.log1p([9, 99, 999]))
    np.testing.assert_array_equal(x[:, 5], [0, 0, 1])
    np.testing.assert_array_equal(x[:, 6], [1, 1, 0])
    assert x[1, 7] == 1 and x[2, 23] == 1
    assert unknown == {'currency': 1, 'payment_format': 1}
    assert x[1, 31] == x[0, 31] == 0  # repeated receiver retains port
    assert x[1, 33] == pytest.approx(np.log1p(60))


def test_future_edges_are_rejected():
    with pytest.raises(ValueError, match='cutoff'):
        p.frozen_features(np.array([[0, 1]]), np.array([60]), np.array([1.]),
                          ['1'], ['1'], np.array(['1', '2']), fitted(), 60)


def test_disk_backed_encoded_features_match_string_path(tmp_path):
    edges = np.array([[0, 1], [0, 1], [2, 1], [2, 2]], dtype=np.int32)
    times = np.array([0, 60, 60, 120])
    amounts = np.array([9., 99., 123., 999.])
    banks = np.array(['010', '2', '010'])
    specification = fitted()
    specification['structure_mean'] = [.1, .2, .3, .4]
    specification['structure_std'] = [1.1, 1.2, 1.3, 1.4]
    specification['node_scaling'] = {'mean': [.123]*30, 'std': [1.234]*30}
    expected = p.frozen_features(edges, times, amounts, ['1', 'new', '2', '1'],
                                 ['1', '1', 'new', '2'], banks, specification, 180)
    mapped = np.lib.format.open_memmap(tmp_path/'features.npy', mode='w+', dtype='float32', shape=(4, 35))
    actual = p.frozen_features(edges, times, amounts, np.array([1, 0, 2, 1]),
        np.array([1, 1, 0, 2]), banks, specification, 180, x=mapped, encoded=True)
    np.testing.assert_array_equal(actual[0], expected[0])
    np.testing.assert_array_equal(actual[1], expected[1])
    assert actual[2] == expected[2]
    mapped._mmap.close()


def test_ranking_uses_all_scores_but_nonself_eligibility_and_stable_ties():
    edges = np.array([[0, 1], [0, 0], [2, 2], [1, 0]])
    rows = np.array([5, 3, 1, 2])
    scores = np.array([.5, .9, .99, .9], dtype=np.float32)
    eligible = np.unique(edges[edges[:, 0] != edges[:, 1]])
    top, values, evidence = p.rank_arrays(edges, rows, scores, 3, 1000, eligible)
    np.testing.assert_array_equal(top, [0, 1])
    np.testing.assert_allclose(values, [.9, .9])
    np.testing.assert_array_equal(evidence, [2, 2])


def test_curated_adapter_filters_before_building_context(tmp_path, monkeypatch):
    model = tmp_path/'model'
    model.mkdir()
    pd.DataFrame({'node_id': [0, 1, 2], 'bank_id': ['010', '2', '3'],
                  'account_id': ['001', '002', '003']}).to_csv(model/'nodes.csv', index=False)
    (model/'features_manifest.json').write_text('{}')
    monkeypatch.setattr(p, 'load_bundle', lambda _: ({}, fitted(), {'model_version': 'v', 'source_sha256': 'train'}))
    curated = tmp_path/'curated'
    (curated/'transfers').mkdir(parents=True)
    (curated/'_SUCCESS').touch()
    frame = pd.DataFrame({'timestamp': pd.to_datetime(['2022-09-01', '2022-09-01 01:00', '2022-09-02'], format='mixed'),
        'sender_bank_id': ['010', '2', '010'], 'sender_account_number': ['001', '002', '001'],
        'receiver_bank_id': ['2', '3', '2'], 'receiver_account_number': ['002', '003', '002'],
        'amount_received': [9., 99., 999.], 'receiving_currency': ['1']*3,
        'payment_format': ['1']*3, 'source_row_id': [0, 1, 2], 'source_sha256': ['abc']*3,
        'transfer_id': ['abc:0', 'abc:1', 'abc:2']})
    frame.iloc[::-1].to_parquet(curated/'transfers'/'data.parquet', index=False)
    out = tmp_path/'context'
    result = p.prepare(curated, model, out, '2022-09-01', '2022-09-02', bank='010')
    assert result['context_rows'] == 1 and result['labels_used'] is False
    assert result['training_source_sha256'] == 'train' and result['source_sha256'] == 'abc'
    np.testing.assert_array_equal(np.load(out/'edge.npy'), [[0, 1]])
    assert (out/'_SUCCESS').exists()
    with pytest.raises(FileExistsError):
        p.prepare(curated, model, out, '2022-09-01', '2022-09-02', bank='010')
    shared = tmp_path/'shared'
    p.prepare(curated, model, shared, '2022-09-01', '2022-09-02')
    assert not np.array_equal(np.load(shared/'node_features.npy')[1], np.load(out/'node_features.npy')[1])

    inference = tmp_path/'inference'
    inference.mkdir()
    np.save(inference/'scores.npy', np.array([.9], dtype=np.float32))
    p.write(inference/'inference.json', dict(result, scores_sha256=p.sha(inference/'scores.npy')))
    (inference/'_SUCCESS').touch()
    ranking = tmp_path/'ranking'
    p.rank(out, inference, ranking, 1000, daily=True, bank='010')
    accounts = pd.read_csv(ranking/'accounts.csv', dtype={'bank_id': str, 'account_id': str})
    assert accounts.bank_id.tolist() == ['010']
    assert accounts.supporting_transfer_id.tolist() == ['abc:0']
    assert len(pd.read_parquet(ranking/'transfers.parquet')) == 1
    # Prediction/export completed before a label table even existed.
    label_path = tmp_path/'labels'
    label_path.mkdir()
    frame[['source_sha256', 'source_row_id']].assign(laundering_mark=[1, 0, 0]).to_parquet(label_path/'part.parquet')
    metrics = p.evaluate(out, ranking, label_path, tmp_path/'evaluation')
    assert metrics['precision_at_k'] == metrics['recall_at_k'] == 1
    assert metrics['eligible'] == metrics['reviewed'] == 1

    # Unknown accounts are appended deterministically; same-time ties follow the
    # physical source row, even when Parquet stores the rows in reverse order.
    frame.loc[0, 'sender_account_number'] = 'new'
    frame.loc[1, 'timestamp'] = frame.loc[0, 'timestamp']
    frame.iloc[::-1].to_parquet(curated/'transfers'/'data.parquet', index=False)
    extra = tmp_path/'extra'
    p.prepare(curated, model, extra, '2022-09-01', '2022-09-02')
    np.testing.assert_array_equal(np.load(extra/'edge.npy'), [[3, 1], [1, 2]])
    np.testing.assert_array_equal(np.load(extra/'row.npy'), [0, 1])
    frame.iloc[[0, 0]].to_parquet(curated/'transfers'/'data.parquet', index=False)
    with pytest.raises(ValueError, match='Duplicate transfer'):
        p.prepare(curated, model, tmp_path/'duplicate', '2022-09-01', '2022-09-02')
