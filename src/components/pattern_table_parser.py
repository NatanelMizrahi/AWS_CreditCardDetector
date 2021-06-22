import re
from pprint import pformat
import aiohttp
import asyncio
import pandas as pd
import regex as re
from bs4 import BeautifulSoup

from config.app_config import DASH_CHARS, URL_TO_TABLE_CLASS
from config.app_config import PATTERN_TABLE_CACHE_PICKLE_PATH
from src.utils.benchmark import timeit
from src.utils.log import getLogger


class PatternTableParser:
    URL_CLASSES = URL_TO_TABLE_CLASS

    def get_credit_card_pattern_table_from_cache(self):
        credit_card_pattern_info = pd.read_pickle(self.cache_path)
        self.df: pd.DataFrame = credit_card_pattern_info['df']
        self.fixed_lengths_df = credit_card_pattern_info['fixed_lengths_df']
        self.interval_idx: pd.IntervalIndex = credit_card_pattern_info['interval_idx']
        self.interval_len_idx: pd.IntervalIndex = credit_card_pattern_info['interval_len_idx']

    def cache_credit_card_pattern(self):
        credit_card_pattern_datastructures = {
            'df': self.df,
            'fixed_lengths_df': self.fixed_lengths_df,
            'interval_idx': self.interval_idx,
            'interval_len_idx': self.interval_len_idx,
        }
        pd.to_pickle(credit_card_pattern_datastructures, self.cache_path)

    def split_prefix_ranges(self):
        self.df["fixed_prefix"] = self.df["ranges"].apply(self.get_fixed_values)
        self.df['prefix_ranges'] = self.df["ranges"].apply(self.get_num_ranges)

    @staticmethod
    def remove_spaces(s: str) -> str:
        return re.sub(r'\s+', '', s)

    @staticmethod
    def to_range(list_of_ints):
        if list_of_ints is None:
            return list_of_ints
        return [(i, i) for i in list_of_ints]

    @staticmethod
    def get_fixed_values(s):
        single_int_pattern = rf'(?<![{DASH_CHARS}])(\b\d+\b)(?![{DASH_CHARS}]),?'
        return [int(e) for e in re.findall(single_int_pattern, PatternTableParser.remove_spaces(s))]

    @staticmethod
    def get_num_ranges(s):
        intervals = []

        for start, end in re.findall(rf'(\d+)[{DASH_CHARS}](\d+)', PatternTableParser.remove_spaces(s)):
            i_start, i_end = int(start), int(end)
            if i_start > i_end:
                i_start, i_end = i_end, i_start
            intervals.append((i_start, i_end))
        return intervals

    def split_length(self):
        self.df["fixed_lengths"] = self.df["length"].apply(self.get_fixed_values).apply(self.to_range)
        self.df['length_ranges'] = self.df["length"].apply(self.get_num_ranges)
        self.df['length_ranges'] = self.df['length_ranges'] + self.df['fixed_lengths']

    @timeit
    def process_IIN(self):
        self.split_prefix_ranges()
        self.split_length()
        self.extract_fixed_prefix_lengths()
        self.extract_interval_prefix_lengths()

    def __init__(self, cache_path=PATTERN_TABLE_CACHE_PICKLE_PATH):
        self.cache_path = cache_path
        self.url_to_html_text_dict = None
        self.fixed_lengths_df: pd.DataFrame = None
        self.df: pd.DataFrame = None
        self.interval_idx: pd.IntervalIndex = None
        self.interval_len_idx: pd.IntervalIndex = None
        self.log = getLogger('[TableParser]')
        try:
            self.get_credit_card_pattern_table_from_cache()
            self.log.info(f'reading credit card pattern data from cache (pickle): {self.cache_path}')
        except FileNotFoundError:
            self.log.info(f'Cache not found ({self.cache_path}).'
                          f'Getting credit card pattern from Wikipedia')
            self.get_credit_card_pattern_tables_from_web()
            self.cache_credit_card_pattern()

    def get_fixed_credit_card_prefixes_list(self):
        return [prefix
                for company_prefixes in self.df["fixed_prefix"][self.df["fixed_prefix"].notna()].to_list()
                for prefix in company_prefixes]

    def prefix_to_len_ranges(self):
        return self.fixed_lengths_df['length_ranges'].to_dict()

    def extract_fixed_prefix_lengths(self):
        fixed_flat_df = self.df[['fixed_prefix', 'length_ranges']].explode('fixed_prefix')
        self.fixed_lengths_df = fixed_flat_df[fixed_flat_df['fixed_prefix'].notna()].set_index('fixed_prefix')

    def extract_interval_prefix_lengths(self):
        intervals_flat_df = self.df[['prefix_ranges', 'length_ranges']].explode('prefix_ranges')
        intervals_flat_df = intervals_flat_df[intervals_flat_df['prefix_ranges'].notna()]
        intervals_flat_df = intervals_flat_df.explode('length_ranges').set_index('prefix_ranges')
        prefix_intervals = intervals_flat_df.index.tolist()
        prefix_interval_length = intervals_flat_df['length_ranges'].tolist()
        self.interval_idx = pd.IntervalIndex.from_tuples(prefix_intervals, closed='both')
        self.interval_len_idx = pd.IntervalIndex.from_tuples(prefix_interval_length, closed='both')

    def get_interval_length_idx(self):
        return self.interval_len_idx

    def get_prefix_interval_idx(self):
        return self.interval_idx

    def get_credit_card_pattern_tables_from_web(self):
        self.get_htmls()
        self.df = self.get_pattern_table_from_htmls()
        self.process_IIN()

    def get_htmls(self):
        asyncio.get_event_loop().run_until_complete(self.get_htmls_task())

    async def fetch(self, session, url):
        self.log.info(f' getting web page:{url}')
        async with session.get(url) as response:
            return await response.text()

    async def get_htmls_task(self):
        urls = self.URL_CLASSES.keys()
        async with aiohttp.ClientSession() as session:
            get_html_tasks = [self.fetch(session, url) for url in urls]
            self.log.info(f'waiting for all GET requests for pages:{pformat(urls)}')
            htmls_texts = await asyncio.gather(*get_html_tasks)
            self.log.info(f'got responses for all GET requests')
            self.url_to_html_text_dict = dict(zip(urls, htmls_texts))

    def get_tables_from_html(self, url, html_text):
        page_tables = self.get_html_tables(url, html_text)
        normalized_tabled = self.get_normalized_credit_card_pattern_tables(page_tables)
        combined_table = pd.concat(normalized_tabled, ignore_index=True)
        return combined_table

    def get_html_tables(self, url, html_text):
        self.log.info(f'parsing tables from page: {url}')
        soup = BeautifulSoup(html_text, 'html.parser')
        class_filter = {'class': re.compile(rf'{self.URL_CLASSES[url]}.*')}
        page_tables = soup.findAll('table', class_filter)
        page_tables_dataframes = pd.read_html(str(page_tables))
        return page_tables_dataframes

    def get_pattern_table_from_htmls(self):
        url_html_pairs = self.url_to_html_text_dict.items()
        pattern_dfs = [self.get_tables_from_html(url, html_text) for url, html_text in url_html_pairs]
        combined_pattern_df = pd.concat(pattern_dfs)
        return combined_pattern_df

    def rename_and_filter_columns(self, df):
        try:
            return self.filter_columns(self.rename_columns(df))
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def filter_columns(df):
        return df[['network', 'ranges', 'length']]

    @staticmethod
    def rename_columns(df):
        def renamer(col_name):
            return re.sub(r'.*?(network|ranges|length).*', r'\1', col_name, flags=re.IGNORECASE).lower()

        return df.rename(renamer, axis='columns')

    def get_normalized_credit_card_pattern_tables(self, page_tables):
        filtered_dfs = [self.rename_and_filter_columns(df) for df in page_tables]
        sanitized_dfs = [self.remove_comments(df) for df in filtered_dfs]
        return sanitized_dfs

    @staticmethod
    def remove_comments(df: pd.DataFrame):
        def remove_non_numric_data(cell):
            return re.sub(rf'\([^\(]*\)|\[[^\[]*\]|[^{DASH_CHARS}\d,]', '', cell)
        df[['ranges', 'length']] = df[['ranges', 'length']].fillna('').applymap(remove_non_numric_data)
        return df

