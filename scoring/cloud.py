"""AWS Batch entry point: stage a frozen snapshot, score, and commit gold outputs.

The completion manifest is the publication boundary. Concurrent attempts write to
separate prefixes; a conditional write elects one complete result. Labels are never
downloaded. Run with python -m scoring.cloud; configuration is supplied by Batch.
"""
import hashlib
import json
import logging
import os
import re
import tempfile
import uuid
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def location(uri):
    parsed = urlparse(uri)
    prefix = parsed.path.strip('/')
    if (parsed.scheme != 's3' or not parsed.netloc or not prefix or parsed.query or
            parsed.fragment or '\\' in prefix or '..' in prefix.split('/')):
        raise ValueError('Expected s3://bucket/nonempty-prefix')
    return parsed.netloc, prefix


def request(start, cutoff, k, bank=''):
    start_day, end_day = (datetime.strptime(value, '%Y-%m-%d') for value in (start, cutoff))
    if start_day.strftime('%Y-%m-%d') != start or end_day.strftime('%Y-%m-%d') != cutoff:
        raise ValueError('Dates must use YYYY-MM-DD')
    if end_day - start_day != timedelta(days=1):
        raise ValueError('Cloud scoring requires one calendar day')
    # This frozen model and curated build are HI-Medium. Do not silently score an
    # empty modern wall-clock date when the daily EventBridge rule fires.
    if not '2022-09-01' <= start <= '2022-09-16':
        raise ValueError('Scoring day is outside HI-Medium primary period')
    if not re.fullmatch(r'[1-9][0-9]*', str(k)) or int(k) > 1000000:
        raise ValueError('K must be an integer between 1 and 1000000')
    if bank and not re.fullmatch(r'[A-Za-z0-9_.-]{1,64}', bank):
        raise ValueError('Invalid bank identifier')
    return dict(start=start, cutoff=cutoff, k=int(k), bank=bank or None, daily=True)


def get_json(s3, bucket, key, optional=False):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if optional and error.response['Error']['Code'] in ('NoSuchKey', '404'):
            return None
        raise
    with response['Body'] as body:
        return json.loads(body.read())


def inventory(s3, bucket, prefix):
    result = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix + '/'):
        for obj in page.get('Contents', []):
            relative = obj['Key'][len(prefix)+1:]
            if relative.endswith('/'):
                continue
            result.append({'key': obj['Key'], 'relative': relative,
                           'etag': obj['ETag'], 'size': obj['Size']})
    return sorted(result, key=lambda item: item['key'])


def stage(s3, bucket, objects, directory):
    root = Path(directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    for obj in objects:
        relative = PurePosixPath(obj['relative'])
        if relative.is_absolute() or '..' in relative.parts or '\\' in str(relative) or ':' in str(relative):
            raise ValueError('Unsafe artifact path')
        path = root.joinpath(*relative.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        # If a producer changes a listed object during staging, fail rather than
        # mixing two curated builds. Stream without holding an object in RAM.
        response = s3.get_object(Bucket=bucket, Key=obj['key'], IfMatch=obj['etag'])
        size = 0
        with response['Body'] as body, path.open('wb') as output:
            for block in iter(lambda: body.read(8 * 1024 * 1024), b''):
                output.write(block)
                size += len(block)
        if size != obj['size']:
            raise ValueError('Artifact length changed during staging')


def run(s3, *, curated_uri, model_uri, gold_uri, model_version, image_revision,
        start, cutoff, k, bank='', execution_id, work_root=None, device='cpu', runner=None):
    options = request(start, cutoff, k, bank)
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', execution_id):
        raise ValueError('Use an execution name containing letters, digits, underscores or hyphens')
    if not re.search(r'@sha256:[0-9a-f]{64}$', image_revision):
        raise ValueError('Cloud scoring requires a digest-pinned image')
    cb, cp = location(curated_uri)
    mb, mp = location(model_uri)
    gb, gp = location(gold_uri)
    s3.head_object(Bucket=cb, Key=cp + '/_SUCCESS')
    s3.head_object(Bucket=mb, Key=mp + '/_SUCCESS')
    transfers = inventory(s3, cb, cp + '/transfers')
    # Spark metadata/CRC files are not model input; only Parquet is staged.
    transfers = [obj for obj in transfers if obj['relative'].endswith('.parquet')]
    models = inventory(s3, mb, mp)
    if not transfers or not models:
        raise ValueError('No completed input artifacts')
    identity = dict(options=options, curated_uri=curated_uri, model_uri=model_uri,
                    model_version=model_version, image=image_revision,
                    transfers=transfers, model_files=models)
    run_id = hashlib.sha256(encode(identity)).hexdigest()
    prefix = f'{gp}/predictions/date={start}/run={run_id}'
    marker = prefix + '/_SUCCESS'
    receipt_key = f'{gp}/executions/{execution_id}.json'
    completed = get_json(s3, gb, marker, optional=True)
    if completed is not None:
        if completed['run_id'] != run_id:
            raise ValueError('Completion manifest identity mismatch')
        s3.put_object(Bucket=gb, Key=receipt_key, Body=encode(completed), ContentType='application/json')
        log.info('Reused completed scoring run: s3://%s/%s', gb, marker)
        return completed

    if runner is None:
        from . import pipeline as runner
    attempt = uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix='scoring-', dir=work_root) as temporary:
        root = Path(temporary)
        curated, model = root/'curated', root/'model'
        log.info('Staging %d transfer files and %d model files', len(transfers), len(models))
        stage(s3, cb, transfers, curated/'transfers')
        stage(s3, mb, models, model)
        (curated/'_SUCCESS').touch()
        _, _, selected = runner.load_bundle(model)
        if selected['model_version'] != model_version:
            raise ValueError('Staged checkpoint is not the configured model version')
        context, inference, ranking = root/'context', root/'inference', root/'ranking'
        log.info('Preparing graph: %s', options)
        runner.prepare(curated, model, context, start, cutoff, options['bank'])
        log.info('Scoring graph on %s', device)
        runner.infer(context, model, inference, device)
        log.info('Ranking eligible accounts')
        runner.rank(context, inference, ranking, options['k'], daily=True, bank=options['bank'])
        for path in (context, inference, ranking):
            if not (path/'_SUCCESS').exists():
                raise RuntimeError(f'Incomplete local stage: {path.name}')
        files = {name: ranking/name for name in ('accounts.csv', 'transfers.parquet', 'ranking.json')}
        files.update({'context_manifest.json': context/'context_manifest.json',
                      'inference.json': inference/'inference.json'})
        artifacts = {}
        for name, path in files.items():
            key = f'{prefix}/attempts/{attempt}/{name}'
            digest = runner.sha(path)
            s3.upload_file(str(path), gb, key, ExtraArgs={'Metadata': {'sha256': digest}})
            artifacts[name] = {'uri': f's3://{gb}/{key}', 'sha256': digest, 'bytes': path.stat().st_size}
        completed = dict(status='completed', run_id=run_id, model_version=model_version,
                         image=image_revision, options=options, input_identity=identity,
                         artifacts=artifacts, completion_uri=f's3://{gb}/{marker}', labels_used=False)
        # A failed upload never publishes _SUCCESS. Different concurrent attempts
        # cannot mix files because the marker names one attempt's immutable objects.
        try:
            s3.put_object(Bucket=gb, Key=marker, Body=encode(completed),
                          ContentType='application/json', IfNoneMatch='*')
        except ClientError as error:
            if error.response['Error']['Code'] not in ('PreconditionFailed', '412'):
                raise
            completed = get_json(s3, gb, marker)
        s3.put_object(Bucket=gb, Key=receipt_key, Body=encode(completed), ContentType='application/json')
        log.info('Scoring complete: s3://%s/%s', gb, receipt_key)
        return completed


def main():
    import boto3
    from botocore.config import Config
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    # Never log the complete environment: it may contain AWS credential fields.
    run(boto3.client('s3', config=Config(retries={'mode': 'standard', 'max_attempts': 5})),
        curated_uri=os.environ['CURATED_URI'], model_uri=os.environ['MODEL_URI'],
        gold_uri=os.environ['GOLD_URI'], model_version=os.environ['MODEL_VERSION'],
        image_revision=os.environ['IMAGE_REVISION'], start=os.environ['SCORING_START'],
        cutoff=os.environ['SCORING_CUTOFF'], k=os.environ['SCORING_K'],
        bank=os.environ.get('SCORING_BANK', ''), execution_id=os.environ['EXECUTION_ID'],
        work_root=os.environ.get('SCORING_WORK_DIR'), device=os.environ.get('SCORING_DEVICE', 'cpu'))


if __name__ == '__main__':
    main()
