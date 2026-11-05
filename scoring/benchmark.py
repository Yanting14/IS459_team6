"""Reproducible local capacity measurements; CSV staging is not EMR acceptance.

Install requirements-dev.txt (including psutil) before running this module.
"""
import argparse
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil

from . import pipeline


def stage_csv(source, model, out):
    """Create a transfer/label-only benchmark snapshot from the frozen IBM CSV.

    This deliberately does not replace the Spark curated job: account-day features,
    quarantine reports, patterns and ring tables remain the responsibility of EMR.
    """
    _, fitted, selected = pipeline.load_bundle(model)
    source = Path(source)
    digest = pipeline.sha(source)
    if digest != selected['source_sha256']:
        raise ValueError('Benchmark CSV must match the frozen training source')
    out = pipeline.fresh(out)
    for name in ('transfers', 'transfer_labels'):
        (out / name).mkdir()
    names = ['timestamp', 'sender_bank_id', 'sender_account_number', 'receiver_bank_id',
             'receiver_account_number', 'amount_received', 'receiving_currency',
             'amount_paid', 'payment_currency', 'payment_format', 'laundering_mark']
    total = 0
    for index, frame in enumerate(pd.read_csv(source, header=0, names=names, dtype=str,
                                             keep_default_na=False, chunksize=200000)):
        if frame.eq('').any().any():
            raise ValueError('Missing fields in frozen benchmark CSV')
        frame.timestamp = pd.to_datetime(frame.timestamp, format='%Y/%m/%d %H:%M', errors='raise')
        for name in ('amount_received', 'amount_paid'):
            frame[name] = pd.to_numeric(frame[name], errors='raise').astype('float64')
            if not np.isfinite(frame[name]).all() or (frame[name] <= 0).any():
                raise ValueError('Invalid amounts in benchmark CSV')
        for name, dictionary in [('receiving_currency', 'currency'), ('payment_currency', 'currency'), ('payment_format', 'format')]:
            if not frame[name].isin(fitted['categorical_dictionaries'][dictionary]).all():
                raise ValueError('Unexpected category in frozen benchmark CSV')
        frame.laundering_mark = pd.to_numeric(frame.laundering_mark, errors='raise').astype('int32')
        if not frame.laundering_mark.isin([0, 1]).all():
            raise ValueError('Invalid benchmark ground truth')
        frame['source_row_id'] = np.arange(total, total + len(frame), dtype=np.int64)
        frame['source_sha256'] = digest
        frame['transfer_id'] = digest + ':' + frame.source_row_id.astype(str)
        frame['source_file'] = source.as_posix()
        frame['row_number'] = frame.source_row_id + 1
        frame['date'] = frame.timestamp.dt.strftime('%Y-%m-%d')
        for date, chunk in frame.groupby('date', sort=False):
            transfers = out / 'transfers' / f'date={date}'
            labels = out / 'transfer_labels' / f'date={date}'
            transfers.mkdir(exist_ok=True)
            labels.mkdir(exist_ok=True)
            chunk.drop(columns=['laundering_mark', 'date']).to_parquet(transfers / f'part-{index:05d}.parquet', index=False)
            chunk[['source_sha256', 'source_row_id', 'transfer_id', 'laundering_mark']].to_parquet(labels / f'part-{index:05d}.parquet', index=False)
        total += len(frame)
        if index % 10 == 0:
            print(json.dumps({'stage': 'benchmark-csv', 'rows': total}), flush=True)
    pipeline.write(out / 'benchmark_snapshot.json', dict(source_sha256=digest, rows=total,
        purpose='Local scoring benchmark only; not a complete Spark curated build', schema_version=2))
    (out / '_SUCCESS').touch()


def measure(command, out):
    """Measure a fresh process to avoid retaining earlier phases' Python heaps."""
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    peak_rss = peak_private = 0
    min_available = psutil.virtual_memory().available
    with out.with_suffix('.log').open('w', encoding='utf-8') as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        process = psutil.Process(child.pid)
        while child.poll() is None:
            try:
                # Windows virtualenv launchers spawn the actual interpreter.
                # Include descendants or the measured footprint is just ~4 MiB.
                memories = [item.memory_info() for item in [process, *process.children(recursive=True)]]
                peak_rss = max(peak_rss, sum(max(m.rss, getattr(m, 'peak_wset', 0)) for m in memories))
                peak_private = max(peak_private, sum(max(getattr(m, 'private', 0), getattr(m, 'peak_pagefile', 0)) for m in memories))
                min_available = min(min_available, psutil.virtual_memory().available)
            except psutil.NoSuchProcess:
                pass
            time.sleep(.25)
    result = dict(command=command, elapsed_seconds=time.perf_counter() - started,
                  peak_rss_bytes=peak_rss, peak_private_bytes=peak_private,
                  minimum_host_available_bytes=min_available, exit_code=child.returncode,
                  platform=platform.platform(), python=sys.version,
                  logical_cpus=psutil.cpu_count(), host_memory_bytes=psutil.virtual_memory().total)
    pipeline.write(out, result)
    print(json.dumps(result), flush=True)
    if child.returncode:
        raise RuntimeError(f'Benchmark failed; see {out.with_suffix(".log")}')
    return result


def run(curated, model, out, date, device='cpu', k=1000):
    """Match cloud.run's in-process prepare -> infer -> rank lifetime."""
    out = Path(out)
    cutoff = str((pd.Timestamp(date) + pd.Timedelta(days=1)).date())
    timings = {}
    actions = [
        ('prepare', lambda: pipeline.prepare(curated, model, out/'context', date, cutoff)),
        ('infer', lambda: pipeline.infer(out/'context', model, out/'inference', device)),
        ('rank', lambda: pipeline.rank(out/'context', out/'inference', out/'ranking', k, daily=True)),
    ]
    for name, action in actions:
        started = time.perf_counter()
        action()
        timings[name] = time.perf_counter() - started
        pipeline.write(out/'phase-seconds.json', timings)
        print(json.dumps({'phase': name, 'elapsed_seconds': timings[name]}), flush=True)


def day(curated, model, out, date, device='cpu', k=1000):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    request = dict(curated=str(Path(curated).resolve()), model=str(Path(model).resolve()),
                   model_bundle_sha256=pipeline.sha(Path(model)/'bundle.json'), date=date, device=device, k=k)
    if (out/'request.json').exists() and pipeline.read(out/'request.json') != request:
        raise ValueError('Benchmark directory belongs to another request')
    pipeline.write(out/'request.json', request)
    common = [sys.executable, '-m']
    jobs = [
        ('scoring', ['scoring.benchmark', 'run', '--curated', curated, '--model', model,
                     '--out', str(out), '--date', date, '--device', device, '--k', str(k)]),
        ('evaluate', ['scoring.pipeline', 'evaluate', '--context', str(out/'context'), '--ranking', str(out/'ranking'),
                      '--labels', str(Path(curated)/'transfer_labels'), '--out', str(out/'evaluation')]),
    ]
    destinations = {'scoring': 'ranking', 'evaluate': 'evaluation'}
    for name, args in jobs:
        report = out / f'{name}-resources.json'
        if report.exists() and pipeline.read(report)['exit_code'] == 0 and (out/destinations[name]/'_SUCCESS').exists():
            print(f'Reusing completed {name}: {out}', flush=True)
            continue
        measure(common + args, report)
    pipeline.write(out/'summary.json', dict(date=date, device=device,
        context=pipeline.read(out/'context'/'context_manifest.json'),
        phase_seconds=pipeline.read(out/'phase-seconds.json'),
        evaluation=pipeline.read(out/'evaluation'/'evaluation.json'),
        resources={name: pipeline.read(out/f'{name}-resources.json') for name, _ in jobs}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    stage = sub.add_parser('stage-csv')
    for name in ('source', 'model', 'out'):
        stage.add_argument('--'+name, required=True)
    for command in ('day', 'run'):
        daily = sub.add_parser(command)
        for name in ('curated', 'model', 'out', 'date'):
            daily.add_argument('--'+name, required=True)
        daily.add_argument('--device', choices=['cpu', 'cuda'], default='cpu')
        daily.add_argument('--k', type=int, default=1000)
    args = vars(parser.parse_args())
    command = args.pop('command')
    {'stage-csv': stage_csv, 'day': day, 'run': run}[command](**args)


if __name__ == '__main__':
    main()
