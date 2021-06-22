import pandas as pd
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

# can be extracted from wiki table
MIN_CREDIT_CARD_LEN = 12
MAX_CREDIT_CARD_LEN = 19

DASH_CHARS = r'‑‐᠆﹣－⁃−'
CREDIT_CARD_DELIMITERS_PATTERN = rf'[{DASH_CHARS} \t,x]'

MAX_RECORD_SIZE_BYTES = 2 ** 20  # 1MB

# parser
MAX_WORKERS = None
PROGRESS_REPORT_INTERVAL = 20
PROGRESS_BAR_CLEAR_SCREEN = False  # performance bottleneck
SAVE_LOCAL_COPY = False
NUM_DOCUMENTS_LIMIT = None
MERGE_DOCS_TO_TXT = False
MATCH_CONTEXT_LEN = 20

# paths
PATTERN_TABLE_CACHE_PICKLE_PATH = "../assets/credit_card_patterns.pkl"
LOCAL_COPY_PATH = '../assets/temp'
MERGED_TXT_PATH = '../assets/cache.txt'
OUTPUT_PATH = '../output/out.csv'

URL_TO_TABLE_CLASS = {
    "https://en.wikipedia.org/wiki/Payment_card_number": "wikitable",
    "http://baymard.com/checkout-usability/credit-card-patterns": "credit-card-pattern-table",
}
