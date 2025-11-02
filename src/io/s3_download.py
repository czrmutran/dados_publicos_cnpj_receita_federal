import os
import threading
import time

import boto3

from src import DATA_FOLDER, UNZIPED_FOLDER_NAME
from src.io.utils import create_folder, display_progress
from src.io.get_last_ref_date import main as get_last_ref_date


n = 8192
dict_status = {}


def _bool_env(var_name: str) -> bool:
    return str(os.getenv(var_name, '')).strip().lower() in ('1', 'true', 'yes', 'on')


def _get_s3_client():
    kwargs = {}
    region = os.getenv('AWS_DEFAULT_REGION')
    if region:
        kwargs['region_name'] = region
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    session_token = os.getenv('AWS_SESSION_TOKEN')
    endpoint_url = os.getenv('AWS_ENDPOINT_URL')
    if endpoint_url:
        kwargs['endpoint_url'] = endpoint_url
    if access_key and secret_key:
        kwargs['aws_access_key_id'] = access_key
        kwargs['aws_secret_access_key'] = secret_key
    if session_token:
        kwargs['aws_session_token'] = session_token
    return boto3.client('s3', **kwargs)


def _stream_download_s3(client, bucket, key, local_path, size_bytes, started_at):  # pragma: no cover
    with open(local_path, 'wb') as f:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        current_bytes = 0
        while True:
            chunk = body.read(n)
            if not chunk:
                break
            f.write(chunk)
            current_bytes += len(chunk)
            running_time_seconds = time.time() - started_at
            speed = current_bytes / running_time_seconds if running_time_seconds > 0 else 0
            eta = (size_bytes - current_bytes) / speed if running_time_seconds > 0 else 0
            global dict_status
            dict_status[os.path.basename(key)] = {
                'total_completed_bytes': current_bytes,
                'file_size_bytes': size_bytes,
                'pct_downloaded': (current_bytes / size_bytes) if size_bytes else 0,
                'started_at': started_at,
                'running_time_seconds': running_time_seconds,
                'speed': speed,
                'eta': eta,
            }
            display_progress(dict_status, started_at=started_at, source='S3 Download', th_to_display=0.05)


def main(ref_date=None):  # pragma: no cover
    bucket = os.getenv('AWS_S3_BUCKET')
    if not bucket:
        print("AWS_S3_BUCKET não definido no .env. Configure o bucket.")
        return False

    ref_date = ref_date or os.getenv('REF_DATE') or get_last_ref_date()
    prefix_base = os.getenv('AWS_S3_PREFIX') or 'dados_abertos_cnpj'
    prefix = f"{prefix_base}/{ref_date}/"

    client = _get_s3_client()

    folder_ref_date_save_csv = os.path.join(DATA_FOLDER, ref_date, UNZIPED_FOLDER_NAME)
    create_folder(folder_ref_date_save_csv)

    print(f"Listando objetos CSV em s3://{bucket}/{prefix}")
    list_keys = []
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get('Contents', [])
        for obj in contents:
            key = obj['Key']
            if key.lower().endswith('.csv'):
                list_keys.append((key, obj.get('Size', 0)))

    if not list_keys:
        print(f"Nenhum arquivo CSV encontrado em s3://{bucket}/{prefix}")
        return False

    started_at = time.time()
    list_threads = []
    for key, size in list_keys:
        filename = os.path.basename(key)
        local_path = os.path.join(folder_ref_date_save_csv, filename)
        t = threading.Thread(target=_stream_download_s3,
                             args=(client, bucket, key, local_path, size, started_at))
        t.start()
        list_threads.append(t)

    print('\n')
    for e, (key, _) in enumerate(list_keys, 1):
        print(f"[{e:3}]/[{len(list_keys):3}] baixando: {os.path.basename(key)}")

    for t in list_threads:
        t.join()
    return True


if __name__ == '__main__':
    main()