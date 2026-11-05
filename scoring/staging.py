"""Disk-backed joins and chronological sorting for the frozen graph adapter."""
from pathlib import Path
from contextlib import ExitStack

import duckdb
import numpy as np
import pandas as pd


def stage(curated, model, out, scratch, origin, cutoff, bank, fitted):
    """Return memory-mapped numeric inputs; never materialise all transfer strings."""
    out, scratch = Path(out), Path(scratch)
    with ExitStack() as cleanup, duckdb.connect(str(scratch / 'graph.duckdb')) as db:
        db.execute("SET memory_limit='2GB'")
        db.execute("SET threads=4")
        db.execute("SET preserve_insertion_order=false")
        db.execute("SET temp_directory=?", [str(scratch / 'spill')])
        db.read_parquet(str(Path(curated) / 'transfers' / '**' / '*.parquet'),
                        hive_partitioning=False).create_view('input_transfers')
        db.execute('''CREATE TABLE visible AS SELECT timestamp, sender_bank_id,
            sender_account_number, receiver_bank_id, receiver_account_number,
            amount_received, receiving_currency, payment_format, source_row_id,
            source_sha256, transfer_id FROM input_transfers
            WHERE timestamp >= ? AND timestamp < ?
              AND (? IS NULL OR sender_bank_id = ? OR receiver_bank_id = ?)''',
            [origin.to_pydatetime(), cutoff.to_pydatetime(), bank, bank, bank])
        count, sources, source, invalid = db.execute('''SELECT count(*),
            count(DISTINCT source_sha256), min(source_sha256),
            count(*) FILTER (WHERE source_sha256 IS NULL OR source_row_id IS NULL
                OR source_row_id < 0 OR source_row_id != floor(source_row_id)
                OR transfer_id IS NULL
                OR transfer_id != source_sha256 || ':' || CAST(source_row_id AS VARCHAR)
                OR sender_bank_id IS NULL OR sender_account_number IS NULL
                OR receiver_bank_id IS NULL OR receiver_account_number IS NULL)
            FROM visible''').fetchone()
        if not count or sources != 1:
            raise ValueError('This adapter requires one nonempty source snapshot')
        if invalid:
            raise ValueError('Transfer IDs do not match source checksum/row, or missing endpoints')
        if db.execute('''SELECT source_row_id FROM visible GROUP BY source_row_id
                         HAVING count(*) > 1 LIMIT 1''').fetchone():
            raise ValueError('Duplicate transfer identities')
        db.read_csv(str(Path(model) / 'nodes.csv'), all_varchar=True).create_view('saved_nodes')
        db.execute('''CREATE TABLE nodes AS SELECT CAST(node_id AS BIGINT) node_id,
                      bank_id, account_id FROM saved_nodes''')
        n, lo, hi, distinct_ids = db.execute('''SELECT count(*), min(node_id),
            max(node_id), count(DISTINCT node_id) FROM nodes''').fetchone()
        ordered_ids = pd.read_csv(Path(model) / 'nodes.csv', usecols=['node_id']).node_id.to_numpy()
        if not n or lo != 0 or hi != n - 1 or distinct_ids != n or not np.array_equal(ordered_ids, np.arange(n)):
            raise ValueError('Node mapping must be contiguous in saved order')
        del ordered_ids
        if db.execute('''SELECT bank_id, account_id FROM nodes
                         GROUP BY ALL HAVING count(*) > 1 LIMIT 1''').fetchone():
            raise ValueError('Duplicate accounts in mapping')
        db.execute('''INSERT INTO nodes
            SELECT ? + row_number() OVER (ORDER BY bank_id, account_id) - 1, bank_id, account_id
            FROM (SELECT sender_bank_id bank_id, sender_account_number account_id FROM visible
                  UNION SELECT receiver_bank_id, receiver_account_number FROM visible) endpoints
            ANTI JOIN nodes USING (bank_id, account_id)''', [n])
        identities = db.execute('SELECT * FROM nodes ORDER BY node_id').fetchdf()
        if len(identities) > np.iinfo(np.int32).max:
            raise ValueError('Graph exceeds int32 node capacity')
        identities.to_csv(out / 'nodes.csv', index=False)
        banks = pd.factorize(identities.bank_id, sort=True)[0]
        del identities
        arrays = {}
        for name, dtype, shape in [('edge', 'int32', (count, 2)), ('time', 'int64', (count,)),
                                    ('row', 'int64', (count,)), ('amount', 'float64', (count,)),
                                    ('currency', 'int16', (count,)), ('format', 'int16', (count,))]:
            directory = out if name in ('edge', 'time', 'row') else scratch
            arrays[name] = np.lib.format.open_memmap(directory / f'{name}.npy', mode='w+', dtype=dtype, shape=shape)
            cleanup.callback(arrays[name]._mmap.close)
        query = '''SELECT s.node_id src, r.node_id dst,
            CAST(epoch(v.timestamp) - epoch(CAST(? AS TIMESTAMP)) AS BIGINT) AS time_seconds,
            v.source_row_id, v.amount_received, v.receiving_currency, v.payment_format
            FROM visible v
            JOIN nodes s ON v.sender_bank_id=s.bank_id AND v.sender_account_number=s.account_id
            JOIN nodes r ON v.receiver_bank_id=r.bank_id AND v.receiver_account_number=r.account_id
            ORDER BY v.timestamp, v.source_row_id'''
        position = 0
        reader = db.execute(query, [origin.to_pydatetime()]).fetch_record_batch(200000)
        for batch in reader:
            frame = batch.to_pandas()
            end = position + len(frame)
            arrays['edge'][position:end] = frame[['src', 'dst']].to_numpy()
            for target, column in [('time', 'time_seconds'), ('row', 'source_row_id'), ('amount', 'amount_received')]:
                arrays[target][position:end] = frame[column].to_numpy()
            for target, column in [('currency', 'receiving_currency'), ('format', 'payment_format')]:
                arrays[target][position:end] = frame[column].map(fitted['categorical_dictionaries'][target]).fillna(0).to_numpy()
            position = end
        if position != count:
            raise ValueError('Endpoint join lost or duplicated transfers')
        for array in arrays.values():
            array.flush()
        cleanup.pop_all()
    return arrays, banks, source
