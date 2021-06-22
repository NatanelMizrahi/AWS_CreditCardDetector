import logging
from contextlib import contextmanager
import time

from pandas import DataFrame

from src.utils.log import getLogger


class Benchmark:
    log = getLogger('[Benchmark]')
    events = []

    @staticmethod
    def print_summary():
        Benchmark.log.info('Bencmark summary:')
        Benchmark.log.info(DataFrame.from_records(Benchmark.events, columns=['function call/label', 'duration']))

    @staticmethod
    @contextmanager
    def timeit_context(label):
        start = time.perf_counter()
        try:
            yield
        finally:
            end = time.perf_counter()
            delta = end-start
            Benchmark.events.append((label, delta))
            Benchmark.log.info(f'{label} took {delta:.2f}s')

    @staticmethod
    def timeit_function(func):
        def timed(*args, **kwargs):
            with Benchmark.timeit_context(f'{func.__name__}'):
                return func(*args, **kwargs)
        return timed

    @staticmethod
    def timeit(func_or_label):
        if hasattr(func_or_label, '__call__'):
            return Benchmark.timeit_function(func_or_label)
        else:
            return Benchmark.timeit_context(func_or_label)


timeit = Benchmark.timeit
