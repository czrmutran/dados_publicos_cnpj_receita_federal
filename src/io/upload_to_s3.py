import os
import threading
import time
import mimetypes

import boto3

import requests

from src import DATA_FOLDER, UNZIPED_FOLDER_NAME
from src.io.utils import display_progress
from src.io.get_last_ref_date import main as get_last_ref_date


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


def _upload_file(client, bucket, local_path, key, started_at):  # pragma: no cover
    size_bytes = os.path.getsize(local_path)
    content_type = mimetypes.guess_type(local_path)[0] or 'application/octet-stream'
    with open(local_path, 'rb') as f:
        client.upload_fileobj(
            f,
            Bucket=bucket,
            Key=key,
            ExtraArgs={'ContentType': content_type},
        )
    running_time_seconds = max(time.time() - started_at, 1)
    speed = size_bytes / running_time_seconds
    global dict_status
    dict_status[os.path.basename(local_path)] = {
        'total_completed_bytes': size_bytes,
        'file_size_bytes': size_bytes,
        'pct_downloaded': 1.0,
        'started_at': started_at,
        'running_time_seconds': running_time_seconds,
        'speed': speed,
        'eta': 0,
    }
    display_progress(dict_status, started_at=started_at, source='S3 Upload', th_to_display=0.05)


def upload_file_path(local_path, ref_date=None, kind=None):  # pragma: no cover
    """Upload um arquivo específico (CSV ou ZIP) para S3.
    kind: 'csv' ou 'zip' (se não informado, infere pela extensão).
    """
    bucket = os.getenv('AWS_S3_BUCKET')
    if not bucket:
        print("AWS_S3_BUCKET não definido no .env. Configure o bucket.")
        return False
    prefix_base = os.getenv('AWS_S3_PREFIX') or 'dados_abertos_cnpj'
    if not ref_date:
        # tenta inferir a ref_date a partir do caminho local (pasta pai de data)
        parent = os.path.basename(os.path.dirname(local_path))
        ref_date = parent
    # decidir subpasta
    if not kind:
        kind = 'zip' if local_path.lower().endswith('.zip') else 'csv'
    key = f"{prefix_base}/{ref_date}/{kind}/{os.path.basename(local_path)}"
    client = _get_s3_client()
    _upload_file(client, bucket, local_path, key, time.time())
    return True


def stream_url_to_s3(link_to_download, ref_date, file_name):  # pragma: no cover
    """Faz o download de uma URL e envia diretamente para o S3 via multipart upload."""
    bucket = os.getenv('AWS_S3_BUCKET')
    if not bucket:
        print("AWS_S3_BUCKET não definido.")
        return False

    prefix_base = os.getenv('AWS_S3_PREFIX') or 'dados_abertos_cnpj'
    key = f"{prefix_base}/{ref_date}/zip/{file_name}"
    client = _get_s3_client()

    with requests.get(link_to_download, stream=True) as r:
        r.raise_for_status()
        file_size_bytes = int(r.headers.get('content-length', 0))
        mpu = client.create_multipart_upload(Bucket=bucket, Key=key, ContentType='application/zip')
        
        parts = []
        part_number = 1
        current_bytes = 0
        started_at = time.time()

        for chunk in r.iter_content(chunk_size=5 * 1024 * 1024):  # 5MB chunks
            if not chunk:
                continue
            part = client.upload_part(Bucket=bucket, Key=key, PartNumber=part_number, UploadId=mpu['UploadId'], Body=chunk)
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
            part_number += 1
            current_bytes += len(chunk)
            
            running_time_seconds = time.time() - started_at
            speed = current_bytes / running_time_seconds if running_time_seconds > 0 else 0
            eta = (file_size_bytes - current_bytes) / speed if file_size_bytes > 0 and speed > 0 else 0
            global dict_status
            dict_status[file_name] = {
                'total_completed_bytes': current_bytes,
                'file_size_bytes': file_size_bytes,
                'pct_downloaded': (current_bytes / file_size_bytes) if file_size_bytes > 0 else 0,
                'started_at': started_at,
                'running_time_seconds': running_time_seconds,
                'speed': speed,
                'eta': eta,
            }
            display_progress(dict_status, started_at=started_at, source='Direct S3 Stream', th_to_display=0.01)

        client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu['UploadId'], MultipartUpload={'Parts': parts})
        print(f"[S3] Stream concluído para: {key}")
    return True


def _collect_files(ref_date, mode='csv'):
    files = []
    if mode in ('csv', 'both'):
        folder_unziped = os.path.join(DATA_FOLDER, ref_date, UNZIPED_FOLDER_NAME)
        if os.path.exists(folder_unziped):
            files += [os.path.join(folder_unziped, f) for f in os.listdir(folder_unziped) if f.lower().endswith('.csv')]
    if mode in ('zip', 'both'):
        folder_zip = os.path.join(DATA_FOLDER, ref_date)
        if os.path.exists(folder_zip):
            files += [os.path.join(folder_zip, f) for f in os.listdir(folder_zip) if f.lower().endswith('.zip')]
    return files


def main(ref_date=None, subfolder='csv'):  # pragma: no cover
    bucket = os.getenv('AWS_S3_BUCKET')
    if not bucket:
        print("AWS_S3_BUCKET não definido no .env. Configure o bucket.")
        return False

    ref_date = ref_date or os.getenv('REF_DATE') or get_last_ref_date()
    prefix_base = os.getenv('AWS_S3_PREFIX') or 'dados_abertos_cnpj'
    mode = os.getenv('S3_UPLOAD_MODE', subfolder)  # csv | zip | both
    if mode == 'zip':
        s3_subfolder = 'zip'
    elif mode == 'both':
        s3_subfolder = ''  # we will build per-file subfolders below
    else:
        s3_subfolder = 'csv'
    base_prefix = f"{prefix_base}/{ref_date}/"

    client = _get_s3_client()

    files = _collect_files(ref_date=ref_date, mode=mode)
    if not files:
        print("Nenhum arquivo para enviar. Verifique se executou io-download e io-unzip.")
        return False

    print(f"Enviando {len(files)} arquivos para s3://{bucket}/{base_prefix}")
    started_at = time.time()
    threads = []
    for local_path in files:
        filename = os.path.basename(local_path)
        # decide subfolder by extension
        if local_path.lower().endswith('.zip'):
            sub = 'zip'
        else:
            sub = 'csv'
        key = f"{base_prefix}{sub}/{filename}" if s3_subfolder == '' else f"{base_prefix}{s3_subfolder}/{filename}"
        t = threading.Thread(target=_upload_file, args=(client, bucket, local_path, key, started_at))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    return True


if __name__ == '__main__':
    main()