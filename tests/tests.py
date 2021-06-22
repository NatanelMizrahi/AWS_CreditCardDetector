import unittest
import re

import numpy as np
import config.app_config

# https://www.web-payment-software.com/test-credit-card-numbers/
from config.app_config import PATTERN_TABLE_CACHE_PICKLE_PATH, MERGED_TXT_PATH
from src.components.doc_parser import DocParser
from src.components.pattern_table_parser import PatternTableParser


class DocParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dp = DocParser()
        cls.default_expected_str = '''
            6699090844295864
            88885554085469629
            88885554085469629
            6699090844295864
            2244606586242045492
            6699090844295864
            2244606586242045492
            6699090844295864
            88885554085469629
            36278281572727
            7777258104615895
            5406535261506582
            3533643370993393
            36278281572727
            5406535261506582
            7777258104615895
            3533643370993393
        '''

    def extract_credit_cards_from_text_helper(self, test_credit_cards_str, expected):
        matches = self.dp.extract_credit_cards_from_text('Test Credit Card Numbers', test_credit_cards_str)
        actual = set(match['credit card number'] for match in matches)
        false_positives = list(actual - expected)
        false_negatives = list(expected - actual)
        self.assertEqual(expected, actual, f'''
                    Incorrect credit card matches:
                    {false_positives=}
                    {false_negatives=}
                ''')

    def test_extract_credit_card_all_documents(self):
        with open(MERGED_TXT_PATH, 'r') as f:
            txt = f.read()
            test_credit_cards_str = self.default_expected_str
            expected = set(re.findall('\d+', test_credit_cards_str))
            self.extract_credit_cards_from_text_helper(txt, expected)

    def test_extract_credit_cards_basic(self):
        test_credit_cards_str = self.default_expected_str
        expected = set(re.findall('\d+', test_credit_cards_str))
        self.extract_credit_cards_from_text_helper(test_credit_cards_str, expected)

    def test_extract_credit_cards_from_onine_example(self):
        test_credit_cards_str = '''
            American Express
            3782 82246 310005
            3714 49635 398431
            Corporate:
            3787 34493 671000
            
            Diners Club
            3852 0000 023237
            3056 9309 025904
            
            Discover
            6011 0009 9100 1201
            6011 1111 1111 1117
            6011 0009 9013 9424
            
            JCB
            3530 1420 1995 5859
            3530 1113 3330 0000
            3566 0020 2036 0505
            
            MasterCard
            5499 7400 0000 0057
            5555 5555 5555 4444
            5105 1051 0510 5100
            
            Visa
            4111 1111 1111 1111
            4242 4242 4242 4242
            4012 8888 8888 1881
            4222 2222 22222
            
            DECOYS:
            7475 1 800 400 400 AIG
            6767 1 800 400 400 AIG
            1234 5678 9101 1121
        '''

        expected = set(["378282246310005",
                        "371449635398431",
                        "378734493671000",
                        "38520000023237",
                        "30569309025904",
                        "6011000991001201",
                        "6011111111111117",
                        "6011000990139424",
                        "3530142019955859",
                        "3530111333300000",
                        "3566002020360505",
                        "5499740000000057",
                        "5555555555554444",
                        "5105105105105100",
                        "4111111111111111",
                        "4242424242424242",
                        "4012888888881881",
                        "4222222222222"])
        self.extract_credit_cards_from_text_helper(test_credit_cards_str, expected)

class TableParserTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ptp = PatternTableParser(cache_path=PATTERN_TABLE_CACHE_PICKLE_PATH)

    def test_fixed_prefix(self):
        fixed_len_df = self.ptp.fixed_lengths_df.T
        print(self.ptp.fixed_lengths_df)
        for prefix_int in [2014, 2149, 60, 6521, 6522]:
            self.assertTrue(prefix_int in fixed_len_df, f'{prefix_int} not found in fixed prefix to length mapping')

        for prefix_int in [560221, 3528, 222100]:
            self.assertTrue(prefix_int not in fixed_len_df, f'{prefix_int} should be in an interval to length mapping')

    def test_interval_prefix(self):
        def interval_prefix_matches_len(prefix, length):
            matching_intervals_mask = self.ptp.interval_idx.contains(prefix)
            matching_interval_len_mask = self.ptp.interval_len_idx.contains(length)
            return np.any(matching_intervals_mask & matching_interval_len_mask)

        legal_interval_prefix_len_pairs = [
            (222100, 16),
            (222222, 16),
            (560225, 16),
            (560223, 16),
        ]
        illegal_interval_prefix_len_pairs = [
            (222100, 17),
            (222222, 15),
            (560225, 17),
            (560223, 15),
        ]
        for prefix, length in legal_interval_prefix_len_pairs:
            self.assertTrue(interval_prefix_matches_len(prefix,length), f'{prefix=} should be legal for {length=}')

        for prefix, length in illegal_interval_prefix_len_pairs:
            self.assertFalse(interval_prefix_matches_len(prefix,length), f'{prefix=} shouldn\'t be legal for {length=}')


if __name__ == '__main__':
    unittest.main()
