import os
import io
import time
import zipfile

import boto3

from src.io.get_last_ref_date import main as get_last_ref_date
from src.io.utils import display_progress


n = 5 * 1024 * 1024  # 5MB chunks
dict_status = {}


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


def _list_zip_keys(client, bucket, base_prefix):
    paginator = client.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix):
        for obj in page.get('Contents', []) or []:
            key = obj['Key']
            if key.lower().endswith('.zip'):
                keys.append(key)
    return keys


def _upload_stream_csv(client, bucket, key, stream, csv_size, started_at):
    mpu = client.create_multipart_upload(Bucket=bucket, Key=key, ContentType='text/csv')
    parts = []
    part_number = 1
    current_bytes = 0

    while True:
        chunk = stream.read(n)
        if not chunk:
            break
        part = client.upload_part(Bucket=bucket, Key=key, PartNumber=part_number,
                                  UploadId=mpu['UploadId'], Body=chunk)
        parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
        part_number += 1
        current_bytes += len(chunk)

        running_time_seconds = time.time() - started_at
        speed = current_bytes / running_time_seconds if running_time_seconds > 0 else 0
        eta = (csv_size - current_bytes) / speed if csv_size > 0 and speed > 0 else 0
        global dict_status
        dict_status[os.path.basename(key)] = {
            'total_completed_bytes': current_bytes,
            'file_size_bytes': csv_size,
            'pct_downloaded': (current_bytes / csv_size) if csv_size else 0,
            'started_at': started_at,
            'running_time_seconds': running_time_seconds,
            'speed': speed,
            'eta': eta,
        }
        display_progress(dict_status, started_at=started_at, source='S3 Unzip->Upload', th_to_display=0.05)

    client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu['UploadId'],
                                     MultipartUpload={'Parts': parts})


def _process_zip_key(client, bucket, ref_date, prefix_base, zip_key):
    # Baixa o ZIP inteiro para memória (BytesIO) e processa cada CSV
    response = client.get_object(Bucket=bucket, Key=zip_key)
    body = response['Body']
    zip_bytes = io.BytesIO(body.read())
    # valida conteúdo
    try:
        zip_bytes.seek(0)
        if not zipfile.is_zipfile(zip_bytes):
            print(f"[UNZIP-S3] Ignorando (não é ZIP válido): {os.path.basename(zip_key)}")
            return
        zf = zipfile.ZipFile(zip_bytes)
    except zipfile.BadZipFile:
        print(f"[UNZIP-S3] Arquivo corrompido/invalid ZIP: {os.path.basename(zip_key)}")
        return

    print(f"[UNZIP-S3] Processando {os.path.basename(zip_key)} com {len(zf.infolist())} entradas")
    for info in zf.infolist():
        if not info.filename.lower().endswith('.csv'):
            continue
        csv_name = os.path.basename(info.filename)
        target_key = f"{prefix_base}/{ref_date}/csv/{csv_name}"
        with zf.open(info) as stream:
            started_at = time.time()
            _upload_stream_csv(client, bucket, target_key, stream, csv_size=info.file_size, started_at=started_at)
            print(f"[S3] CSV enviado: s3://{bucket}/{target_key}")


def main(ref_date=None):  # pragma: no cover
    bucket = os.getenv('AWS_S3_BUCKET')
    if not bucket:
        print("AWS_S3_BUCKET não definido no .env. Configure o bucket.")
        return False

    ref_date = ref_date or os.getenv('REF_DATE') or get_last_ref_date()
    prefix_base = os.getenv('AWS_S3_PREFIX') or 'dados_abertos_cnpj'
    zip_prefix = f"{prefix_base}/{ref_date}/zip/"

    client = _get_s3_client()
    zip_keys = _list_zip_keys(client, bucket, zip_prefix)
    if not zip_keys:
        print(f"Nenhum ZIP encontrado em s3://{bucket}/{zip_prefix}")
        return False

    print(f"Descompactando {len(zip_keys)} ZIPs de s3://{bucket}/{zip_prefix} -> csv/")
    for i, zip_key in enumerate(zip_keys, 1):
        print(f"[{i:3}/{len(zip_keys):3}] {os.path.basename(zip_key)}")
        _process_zip_key(client, bucket, ref_date, prefix_base, zip_key)

    return True


if __name__ == '__main__':
    main()