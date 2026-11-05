"""Publish an immutable local model bundle to the versioned model store."""
import argparse
import json
from pathlib import Path

from botocore.exceptions import ClientError

from .cloud import location
from .pipeline import load_bundle, sha


def publish(s3, bundle, destination):
    bundle = Path(bundle)
    _, _, selected = load_bundle(bundle)
    bucket, prefix = location(destination)
    manifest = json.loads((bundle/'bundle.json').read_text())
    names = [*manifest['files'], 'bundle.json']
    for name in names:
        if Path(name).name != name or name in ('.', '..') or '\\' in name or ':' in name:
            raise ValueError('Invalid model artifact name')
        path = bundle/name
        digest = sha(path)
        try:
            with path.open('rb') as body:
                s3.put_object(Bucket=bucket, Key=f'{prefix}/{name}', Body=body,
                              Metadata={'sha256': digest}, IfNoneMatch='*')
        except ClientError as error:
            if error.response['Error']['Code'] not in ('412', 'PreconditionFailed'):
                raise
            existing = s3.head_object(Bucket=bucket, Key=f'{prefix}/{name}')
            if existing.get('Metadata', {}).get('sha256') != digest:
                raise ValueError(f'Existing model artifact differs: {name}; choose a new prefix') from error
    try:
        s3.put_object(Bucket=bucket, Key=prefix+'/_SUCCESS', Body=b'', IfNoneMatch='*')
    except ClientError as error:
        if error.response['Error']['Code'] not in ('412', 'PreconditionFailed'):
            raise
    return {'model_version': selected['model_version'], 'model_uri': destination}


def main():
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--destination', required=True)
    args = parser.parse_args()
    print(json.dumps(publish(boto3.client('s3'), args.bundle, args.destination), indent=2))


if __name__ == '__main__':
    main()
