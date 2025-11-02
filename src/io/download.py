import os
import threading
import time
import zipfile

import requests

from src.io.get_files_dict import main as get_files_dict
from src.io.utils import create_folder, display_progress
from src.io.s3_download import main as s3_download_main
from src.io.upload_to_s3 import upload_file_path, stream_url_to_s3

n = 8192

dict_status = {}


def main():  # pragma: no cover
    use_s3 = str(os.getenv('USE_S3_DOWNLOAD', '')).strip().lower() in ('1', 'true', 'yes', 'on')
    direct_to_s3 = str(os.getenv('DIRECT_TO_S3', '')).strip().lower() in ('1', 'true', 'yes', 'on')
    if use_s3:
        print('[DOWNLOAD] Usando S3 para baixar CSVs')
        s3_download_main()
        return

    dict_files_dict = get_files_dict()
    create_folder(dict_files_dict['folder_ref_date_save_zip'])
    started_at = time.time()
    list_threads = []
    list_needs_download = []
    for tbl in dict_files_dict.keys():
        _dict = dict_files_dict[tbl]
        # skip 'folder_ref_date_save_zip' key (not a dict)
        if isinstance(_dict, str):
            continue
        for file in _dict.keys():
            link_to_download = _dict[file]['link_to_download']
            path_save_file = _dict[file]['path_save_file']
            file_size_bytes = _dict[file]['file_size_bytes']

            # check if file is already downloaded in order to not downloaded again
            try:
                # try to open file
                archive = zipfile.ZipFile(path_save_file, 'r')
                print(f"'{path_save_file:60}' - [GO] already downloaded")
                continue
            except zipfile.BadZipFile:
                # if file cannot be opened then it is not ready
                print(f"'{path_save_file:60}' - [NO GO] not fully downloaded")
                list_needs_download.append(path_save_file)
            except FileNotFoundError:
                print(f"'{path_save_file:60}' - [NO GO] file not exists")
                list_needs_download.append(path_save_file)

            if direct_to_s3:
                # Envia stream diretamente para S3 (ZIP)
                ref_date = os.path.basename(os.path.dirname(path_save_file))
                t = threading.Thread(target=stream_url_to_s3, args=(link_to_download, ref_date, file))
                t.start()
                list_threads.append(t)
            else:
                t = threading.Thread(target=download_file,
                                     args=(file, link_to_download, path_save_file, file_size_bytes, started_at))
                t.start()
                list_threads.append(t)

    print('\n' * 3)
    if list_needs_download:
        for e, _file in enumerate(list_needs_download, 1):
            print(f"[{e:3}]/[{len(list_needs_download):3}] downloading file: {_file}")
    else:
        print(f"All files are already downloaded")

    for t in list_threads:
        t.join()


def download_file(file_name, link_to_download, path_save_file, file_size_bytes, started_at):  # pragma: no cover
    """
    Download a file into local system
    :param file_name: file name
    :param link_to_download: link to download file
    :param path_save_file: path to save file
    :param file_size_bytes: size in bytes of file
    :return:
    """
    with requests.get(link_to_download, stream=True) as r:
        r.raise_for_status()
        current_file_downloaded_bytes = 0
        with open(path_save_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=n):
                f.write(chunk)
                current_file_downloaded_bytes += len(chunk)
                running_time_seconds = time.time() - started_at
                speed = current_file_downloaded_bytes / running_time_seconds if running_time_seconds > 0 else 0
                eta = (file_size_bytes - current_file_downloaded_bytes) / speed if running_time_seconds > 0 else 0
                global dict_status
                dict_status[file_name] = {'total_completed_bytes': current_file_downloaded_bytes,
                                          'file_size_bytes': file_size_bytes,
                                          'pct_downloaded': current_file_downloaded_bytes / file_size_bytes,
                                          'started_at': started_at,
                                          'running_time_seconds': running_time_seconds,
                                          'speed': speed,
                                          'eta': eta,
                                          }
                display_progress(dict_status, started_at=started_at, source='Download', th_to_display=0.05)

        # upload para S3, se habilitado
        upload_enabled = str(os.getenv('UPLOAD_TO_S3', '')).strip().lower() in ('1', 'true', 'yes', 'on')
        if upload_enabled:
            ref_date = os.path.basename(os.path.dirname(path_save_file))
            try:
                upload_file_path(local_path=path_save_file, ref_date=ref_date, kind='zip')
                print(f"[S3] Enviado: {os.path.basename(path_save_file)} para {ref_date}/zip")
            except Exception as e:
                print(f"[S3] Falha ao enviar {path_save_file}: {e}")


if __name__ == '__main__':
    main()
