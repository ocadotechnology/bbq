import functools
import logging
from google.appengine.api import memcache


def cached(time=1200):
    """
    Decorator that caches the result of a method for the specified time in
    seconds.

    IMPORTANT: If function returns None value, result will not be cached.
    In that case decorated function will be called each time,
    unless return something else than None.

    WARNING: this uses name of the function and passed parameters as a cache
    key.
    'self' param and another ones depends on given __str__ implementations.
    Be aware if you annotate functions used in different context,
    ensure that both of them generate different key.


    Usage:
      @cached(time=1200)
      def functionToCache(arguments):
        ...

    """

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            key = '%s%s%s' % (function.__name__, str(args), str(kwargs))
            value = memcache.get(key)
            logging.debug('Memcache: key %s %s found', key,
                          'not' if value is None else '')
            if value is None:
                value = function(*args, **kwargs)
                memcache.set(key, value, time=time)
            return value

        return wrapper

    return decorator
