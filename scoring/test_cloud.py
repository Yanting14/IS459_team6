import hashlib
import io
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from scoring import cloud


class FakeS3:
    """In-memory S3 with conditional reads/writes, not a model implementation."""
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.reads = []
        self.fail_upload = False
        self.race = False

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, **kwargs):
        if IfNoneMatch and (Bucket, Key) in self.objects:
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[Bucket, Key] = Body.read() if hasattr(Body, 'read') else Body
        self.metadata[Bucket, Key] = kwargs.get('Metadata', {})
        return {}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        return {'ETag': hashlib.md5(self.objects[Bucket, Key]).hexdigest(),
                'Metadata': self.metadata.get((Bucket, Key), {})}

    def get_object(self, Bucket, Key, IfMatch=None):
        self.reads.append((Bucket, Key))
        if (Bucket, Key) not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        data = self.objects[Bucket, Key]
        if IfMatch and IfMatch != hashlib.md5(data).hexdigest():
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'GetObject')
        return {'Body': io.BytesIO(data)}

    def get_paginator(self, name):
        return self

    def paginate(self, Bucket, Prefix):
        yield {'Contents': [{'Key': key, 'ETag': hashlib.md5(value).hexdigest(), 'Size': len(value)}
                            for (bucket, key), value in self.objects.items()
                            if bucket == Bucket and key.startswith(Prefix)]}

    def upload_file(self, path, bucket, key, **kwargs):
        if self.fail_upload:
            raise RuntimeError('Upload interrupted')
        self.put_object(Bucket=bucket, Key=key, Body=Path(path).read_bytes())
        if self.race and key.endswith('/inference.json'):
            marker = key.split('/attempts/')[0] + '/_SUCCESS'
            run_id = marker.split('run=')[1].split('/')[0]
            self.put_object(Bucket=bucket, Key=marker,
                            Body=cloud.encode({'run_id': run_id, 'winner': 'other-attempt'}))


class FakeRunner:
    calls = []

    @staticmethod
    def sha(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def load_bundle(self, path):
        return {}, {}, {'model_version': 'model-v1'}

    def prepare(self, curated, model, context, start, cutoff, bank):
        self.calls.append('prepare')
        assert not (curated/'transfer_labels').exists()
        context.mkdir()
        (context/'context_manifest.json').write_text('{}')
        (context/'_SUCCESS').touch()

    def infer(self, context, model, inference, device):
        self.calls.append('infer')
        inference.mkdir()
        (inference/'inference.json').write_text('{}')
        (inference/'_SUCCESS').touch()

    def rank(self, context, inference, ranking, k, daily, bank):
        self.calls.append('rank')
        assert daily
        ranking.mkdir()
        for name in ('accounts.csv', 'transfers.parquet', 'ranking.json', '_SUCCESS'):
            (ranking/name).write_text('{}')


@pytest.fixture
def scenario(tmp_path):
    s3 = FakeS3()
    for bucket, key in [('curated', 'v2/_SUCCESS'), ('curated', 'v2/transfers/date=2022-09-01/part.parquet'),
                        ('curated', 'v2/transfer_labels/part.parquet'), ('models', 'gin/_SUCCESS'),
                        ('models', 'gin/bundle.json')]:
        s3.put_object(Bucket=bucket, Key=key, Body=b'fixture')
    runner = FakeRunner()
    runner.calls = []
    args = dict(curated_uri='s3://curated/v2', model_uri='s3://models/gin', gold_uri='s3://gold/v2',
                model_version='model-v1', image_revision='repo@sha256:'+'a'*64,
                start='2022-09-14', cutoff='2022-09-15', k=1000, execution_id='manual-test',
                work_root=tmp_path, runner=runner)
    return s3, args, runner


def test_publish_then_reuse_without_scoring_or_labels(scenario):
    s3, args, runner = scenario
    completed = cloud.run(s3, **args)
    assert runner.calls == ['prepare', 'infer', 'rank']
    assert len(completed['artifacts']) == 5 and completed['labels_used'] is False
    assert not any('transfer_labels' in key for _, key in s3.reads)
    assert cloud.run(s3, **args) == completed
    assert len(runner.calls) == 3
    receipt = json.loads(s3.objects['gold', 'v2/executions/manual-test.json'])
    assert receipt['run_id'] == completed['run_id']


def test_failed_upload_never_commits_and_new_attempt_can_retry(scenario):
    s3, args, runner = scenario
    s3.fail_upload = True
    with pytest.raises(RuntimeError, match='interrupted'):
        cloud.run(s3, **args)
    assert not any(bucket == 'gold' and key.endswith('_SUCCESS') for bucket, key in s3.objects)
    s3.fail_upload = False
    assert cloud.run(s3, **args)['status'] == 'completed'


def test_concurrent_attempt_uses_winners_manifest(scenario):
    s3, args, _ = scenario
    s3.race = True
    result = cloud.run(s3, **args)
    assert result['winner'] == 'other-attempt'
    assert json.loads(s3.objects['gold', 'v2/executions/manual-test.json']) == result


def test_changed_input_or_image_gets_a_new_run(scenario):
    s3, args, _ = scenario
    first = cloud.run(s3, **args)
    s3.put_object(Bucket='curated', Key='v2/transfers/date=2022-09-01/part.parquet', Body=b'new')
    second = cloud.run(s3, **args)
    third = cloud.run(s3, **dict(args, image_revision='repo@sha256:'+'b'*64))
    assert len({item['run_id'] for item in (first, second, third)}) == 3


def test_missing_input_marker_and_changed_download_fail(scenario, tmp_path):
    s3, args, runner = scenario
    objects = cloud.inventory(s3, 'models', 'gin')
    s3.put_object(Bucket='models', Key='gin/bundle.json', Body=b'changed')
    with pytest.raises(ClientError):
        cloud.stage(s3, 'models', objects, tmp_path/'staged')
    del s3.objects['curated', 'v2/_SUCCESS']
    with pytest.raises(ClientError):
        cloud.run(s3, **args)
    assert not runner.calls


@pytest.mark.parametrize('start,cutoff,k', [('2026-11-05', '2026-11-06', 1000),
    ('2022-09-14', '2022-09-16', 1000), ('2022-09-14', '2022-09-15', '1.5')])
def test_invalid_scoring_request_rejected(start, cutoff, k):
    with pytest.raises(ValueError):
        cloud.request(start, cutoff, k)


def test_staging_refuses_path_traversal(scenario, tmp_path):
    s3, _, _ = scenario
    with pytest.raises(ValueError, match='Unsafe'):
        cloud.stage(s3, 'models', [{'relative': '../outside', 'key': 'x'}], tmp_path/'stage')


def test_model_publication_is_immutable_and_resumable(tmp_path, monkeypatch):
    from scoring import publish
    monkeypatch.setattr(publish, 'load_bundle', lambda _: ({}, {}, {'model_version': 'v1'}))
    bundle = tmp_path/'bundle'
    bundle.mkdir()
    (bundle/'selected_model.pt').write_bytes(b'weights')
    (bundle/'bundle.json').write_text(json.dumps({'files': {'selected_model.pt': 'fixture'}}))
    s3 = FakeS3()
    first = publish.publish(s3, bundle, 's3://models/gin')
    assert ('models', 'gin/_SUCCESS') in s3.objects
    assert publish.publish(s3, bundle, 's3://models/gin') == first
    (bundle/'selected_model.pt').write_bytes(b'changed-weights')
    with pytest.raises(ValueError, match='differs'):
        publish.publish(s3, bundle, 's3://models/gin')
    assert s3.objects['models', 'gin/selected_model.pt'] == b'weights'
