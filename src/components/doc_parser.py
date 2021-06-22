import numpy as np
import pandas as pd
import regex as re

from config.app_config import MIN_CREDIT_CARD_LEN, MAX_CREDIT_CARD_LEN, CREDIT_CARD_DELIMITERS_PATTERN, \
    MATCH_CONTEXT_LEN
from src.components.pattern_table_parser import PatternTableParser
from src.utils.Trie import Trie
from src.utils.benchmark import timeit
from src.utils.log import getLogger
from src.utils.utils import checkLuhn


class DocParser:
    possible_credit_card_pattern = \
        rf'(?<=\D)(?:[0-9]{CREDIT_CARD_DELIMITERS_PATTERN}*){{{MIN_CREDIT_CARD_LEN},{MAX_CREDIT_CARD_LEN}}}(?=\D)'

    def __init__(self):
        self.table_parser = PatternTableParser()
        self.interval_length_idx: pd.IntervalIndex = self.table_parser.get_interval_length_idx()
        self.prefix_interval_idx: pd.IntervalIndex = self.table_parser.get_prefix_interval_idx()
        self.possible_interval_lengths = self.get_possible_interval_lengths()
        self.lengthFilterPattern = re.compile(DocParser.possible_credit_card_pattern)
        self.fixed_pattern = self.get_wiki_credit_card_prefix_pattern()
        self.prefix_to_len_ranges = self.table_parser.prefix_to_len_ranges()
        self.log = getLogger('[DocParser]')
        self.matches = []

    @timeit
    def get_wiki_credit_card_prefix_pattern(self):
        prefix_tree = Trie()
        for prefix in self.table_parser.get_fixed_credit_card_prefixes_list():
            prefix_tree.add(str(prefix))
        prefix_pattern = prefix_tree.pattern()
        return re.compile(rf'^{prefix_pattern}')

    def filter_by_length(self, text):
        return self.lengthFilterPattern.finditer(text, overlapped=True)

    @staticmethod
    def get_context(text, match):
        start_pos, end_pos = match.start(), match.end()
        context_substr = text[start_pos-MATCH_CONTEXT_LEN:end_pos + MATCH_CONTEXT_LEN].replace('\n', ' ')
        return f'... {context_substr}...'

    @staticmethod
    def overlaps(match, prev_match_end_pos):
        start_pos = match.start()
        return start_pos < prev_match_end_pos

    def extract_credit_cards_from_text(self, filename, text):
        with timeit(f'Extracting: {filename}'):
            matches = []
            num_matches = 0
            prev_match_end_pos = 0  # save the position of the previous match to prevent overlaps
            self.log.info('Extracting credit card numbers from file: ' + filename)
            for possible_match in self.filter_by_length(text):
                if self.overlaps(possible_match, prev_match_end_pos):
                    continue
                num_matches += 1
                s = possible_match.group()
                digits_str = re.sub(r'\D', '', s)
                is_valid_len_and_prefix = self.is_valid_credit_card_prefix(digits_str)
                if is_valid_len_and_prefix:
                    prev_match_end_pos = possible_match.end()
                matches.append({
                    'filename': filename,
                    'credit card number': digits_str,
                    'valid': is_valid_len_and_prefix,
                    'valid_checksum': checkLuhn(digits_str),
                    'file match #': num_matches,
                    'context': self.get_context(text, possible_match)
                })
            return matches

    def is_interval_prefix_length_valid(self, prefix, length):
        prefix = int(prefix)
        matching_intervals_mask = self.prefix_interval_idx.contains(prefix)
        matching_interval_len_mask = self.interval_length_idx.contains(length)
        return np.any(matching_intervals_mask & matching_interval_len_mask)

    def is_fixed_prefix_length_valid(self, prefix, length):
        prefix_as_int = int(prefix)
        for low, high in self.prefix_to_len_ranges[prefix_as_int]:
            if low <= length <= high:
                return True
        return False

    def is_valid_credit_card_prefix(self, digits_str):
        if self.matches_fixed_prefix_pattern(digits_str):
            return True
        if self.matches_interval_prefix_pattern(digits_str):
            return True
        return False

    def matches_fixed_prefix_pattern(self, digits_str):
        n_digits = len(digits_str)
        for prefix_match in self.fixed_pattern.finditer(digits_str):
            prefix = prefix_match.group()
            if self.is_fixed_prefix_length_valid(prefix, n_digits):
                return True
        return False

    def matches_interval_prefix_pattern(self, digits_str):
        for interval_prefix_len in self.possible_interval_lengths:
            prefix = digits_str[:interval_prefix_len]
            if self.is_interval_prefix_length_valid(prefix, interval_prefix_len):
                return True
        return False

    def get_possible_interval_lengths(self):
        possible_interval_lengths = set()
        for start, end in self.prefix_interval_idx.to_tuples():
            start_len = len(str(start))
            end_len = len(str(end))
            possible_interval_lengths.add(start_len)
            possible_interval_lengths.add(end_len)
        return list(possible_interval_lengths)
