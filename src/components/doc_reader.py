import os
import docx2txt
import textract

from config.app_config import LOCAL_COPY_PATH, SAVE_LOCAL_COPY
from src.utils.benchmark import timeit
from src.utils.log import getLogger


class DocReader:
    def __init__(self):
        self.log = getLogger('[DocParser]')

    def parse_file_data(self, filename, data):
        with timeit(f'{filename} S3 read to text'):
            local_copy_path = f'{LOCAL_COPY_PATH}/{filename}'
            with open(local_copy_path, 'w+b') as copy:
                copy.write(data)
            try:
                return docx2txt.process(local_copy_path)
            except Exception as e_docx:
                self.log.info(f'failed to parse {filename} with docx2txt, trying other formats. Error: {e_docx}')
                try:
                    return textract.process(local_copy_path)
                except Exception as e_other:
                    self.log.info(f'failed to parse {filename}, Error: {e_other}')
            finally:
                if not SAVE_LOCAL_COPY:
                    os.remove(local_copy_path)
