import os
import threading
import time
import zipfile

from src import UNZIPED_FOLDER_NAME
from src.io.get_files_dict import main as get_files_dict
from src.io.utils import create_folder, check_if_folder_is_empty, display_progress
from src.io.upload_to_s3 import upload_file_path

dict_status = {}
chunk_size = 1024 * 1024 * 2


def main(folder_to_unzip=None):  # pragma: no cover
    dict_files_dict = get_files_dict()
    folder_to_unzip = folder_to_unzip or dict_files_dict['folder_ref_date_save_zip']
    started_at = time.time()
    is_empty = check_if_folder_is_empty(folder_to_unzip)
    if is_empty:
        print('Folder is empty.. exiting.. nothing to unzip')
        return False

    folder_ref_date_save_zip = dict_files_dict['folder_ref_date_save_zip']
    files_folder_ref_date_save_zip = [os.path.join(folder_ref_date_save_zip, file) for file in
                                      os.listdir(folder_ref_date_save_zip)]
    files_folder_ref_date_save_zip = [file for file in files_folder_ref_date_save_zip if file.endswith('.zip')]
    folder_ref_date_save_unziped = os.path.join(folder_ref_date_save_zip, UNZIPED_FOLDER_NAME)
    create_folder(folder_ref_date_save_unziped)

    list_threads = []
    for file in files_folder_ref_date_save_zip:
        t = threading.Thread(target=unzip_file, args=(file, folder_ref_date_save_unziped, started_at))
        t.start()
        list_threads.append(t)

    for t in list_threads:
        t.join()


def unzip_file(file, folder_ref_date_save_unziped, started_at):  # pragma: no cover
    with zipfile.ZipFile(file, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            file_size_bytes = zip_ref.getinfo(file_name).file_size
            file_target = os.path.join(folder_ref_date_save_unziped, file_name)

            with open(file_target, 'wb') as outfile:
                member_fd = zip_ref.open(file_name)
                current_file_unziped_bytes = 0
                while 1:
                    x = member_fd.read(chunk_size)
                    if not x:
                        break
                    current_file_unziped_bytes += outfile.write(x)
                    running_time_seconds = time.time() - started_at
                    speed = current_file_unziped_bytes / running_time_seconds if running_time_seconds > 0 else 0
                    eta = (file_size_bytes - current_file_unziped_bytes) / speed if running_time_seconds > 0 else 0
                    global dict_status
                    dict_status[file_name] = {'total_completed_bytes': current_file_unziped_bytes,
                                              'file_size_bytes': file_size_bytes,
                                              'pct_downloaded': current_file_unziped_bytes / file_size_bytes,
                                              'started_at': started_at,
                                              'running_time_seconds': running_time_seconds,
                                              'speed': speed,
                                              'eta': eta,
                                              }
                    display_progress(dict_status, started_at=started_at, source='Unzip', th_to_display=0.01)

            # upload CSV para S3 se habilitado
            upload_enabled = str(os.getenv('UPLOAD_TO_S3', '')).strip().lower() in ('1', 'true', 'yes', 'on')
            mode = str(os.getenv('S3_UPLOAD_MODE', 'csv')).strip().lower()
            if upload_enabled and mode in ('csv', 'both'):
                ref_date = os.path.basename(os.path.dirname(folder_ref_date_save_unziped))
                try:
                    upload_file_path(local_path=file_target, ref_date=ref_date, kind='csv')
                    print(f"[S3] Enviado CSV: {os.path.basename(file_target)} para {ref_date}/csv")
                except Exception as e:
                    print(f"[S3] Falha ao enviar CSV {file_target}: {e}")


if __name__ == '__main__':
    main()
