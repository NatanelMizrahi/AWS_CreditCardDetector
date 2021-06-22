import asyncio
import concurrent.futures
from pprint import pformat
from queue import Queue
from threading import Lock

from pandas import DataFrame

from config.app_config import MAX_WORKERS, MERGE_DOCS_TO_TXT, MERGED_TXT_PATH, PROGRESS_REPORT_INTERVAL
from src.components.aws_s3_client import AWS3Client
from src.components.doc_parser import DocParser
from src.components.doc_reader import DocReader
from src.utils.benchmark import timeit
from src.utils.log import getLogger
from src.utils.progress_bar import printProgressBar


class DocParserExecutor:
    progressbar_lock = Lock()

    def __init__(self, s3_client: AWS3Client):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.s3_client = s3_client
        self.log = getLogger('[Executor]')
        self.loop = asyncio.get_event_loop()
        self.doc_reader = DocReader()
        self.doc_parser = DocParser()
        self.progress = 0
        self.prev_progress = 0
        self.num_tasks = 0
        self.write_queue = Queue()  # used to merge text for tests

    def exec(self, func, *args):
        return self.loop.run_in_executor(self.executor, func, *args)

    async def extract_doc_cc_numbers(self, filename):
        with timeit(f'Process {filename}'):
            self.log.info(f'Processing {filename}')
            file_data = await self.s3_client.get_file(filename)
            file_text = await self.exec(self.doc_reader.parse_file_data, filename, file_data)
            if MERGE_DOCS_TO_TXT:
                self.write_queue.put(file_text)
            credit_cards = await self.exec(self.doc_parser.extract_credit_cards_from_text, filename, file_text)
            if credit_cards:
                credit_card_nos = [cc["credit card number"] for cc in credit_cards]
                self.log.info(f'found the following credit card numbers in {filename}: {credit_card_nos}')
            self.report_progress()
            return credit_cards

    def cache_text(self):
        if MERGE_DOCS_TO_TXT:
            with open(MERGED_TXT_PATH, 'a+', encoding='utf-8') as merged_txt:
                while self.write_queue.qsize():
                    merged_txt.write(self.write_queue.get())

    async def extract_all_cc_numbers(self, docs):
        self.num_tasks = len(docs)
        self.log.info('extracting credit card numbers from the following docs:')
        self.log.info(pformat(docs))
        doc_cc_futures = [self.extract_doc_cc_numbers(filename) for filename in docs]
        self.log.info('waiting for executor tasks')
        credit_card_lists = await asyncio.gather(*doc_cc_futures)
        credit_cards_info = [cc_number_info for sublist in credit_card_lists for cc_number_info in sublist]
        self.log.info('found the following credit card numbers:')
        self.log.info([cc['credit card number'] for cc in credit_cards_info])
        self.cache_text()
        credit_cards_info_df = DataFrame(credit_cards_info).sort_values(["filename", "file match #"])
        return credit_cards_info_df

    def report_progress(self):
        report = False
        with self.progressbar_lock:
            self.progress += 1
            if (self.progress == self.prev_progress + PROGRESS_REPORT_INTERVAL) or (self.progress == self.num_tasks):
                self.prev_progress = self.progress
                report = True
        if report:
            printProgressBar(self.progress, self.num_tasks)
