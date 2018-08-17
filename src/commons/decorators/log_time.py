import logging
import time
from contextlib import contextmanager


def log_time(method):
    def timed(*args, **kw):
        with measure_time_and_log(method.__name__):
            return method(*args, **kw)

    return timed

@contextmanager
def measure_time_and_log(text):
    start = time.time()
    yield
    end = time.time()
    logging.info('%s finished after %2.2f', text, end-start)
