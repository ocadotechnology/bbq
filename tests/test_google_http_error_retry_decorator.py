import unittest

from apiclient.errors import HttpError

from src.commons.decorators.google_http_error_retry import \
    google_http_error_retry
from mock import patch, mock


class TestGoogleHttpErrorRetryDecorator(unittest.TestCase):

    class TestClass(object):
        def counter_for_400(self):
            pass

        def counter_for_404(self):
            pass

        def counter_for_500(self):
            pass

        @google_http_error_retry(tries=3, delay=0, backoff=1)
        def raise_HttpError400(self):
            self.counter_for_400()
            raise HttpError(mock.Mock(status=400), 'Bad request')

        @google_http_error_retry(tries=3, delay=0, backoff=1)
        def raise_HttpError404(self):
            self.counter_for_404()
            raise HttpError(mock.Mock(status=404), 'Not Found')

        @google_http_error_retry(tries=3, delay=0, backoff=1)
        def raise_HttpError500(self):
            self.counter_for_500()
            raise HttpError(mock.Mock(status=500), 'Internal error')

    @patch.object(TestClass, "counter_for_400")
    def test_that_http_400_errors_are_not_retried(self, counter_for_400):
        # when
        self.execute_function_and_suppress_exceptions(
            TestGoogleHttpErrorRetryDecorator.TestClass().raise_HttpError400)
        # then
        self.assertEquals(1, counter_for_400.call_count)\


    @patch.object(TestClass, "counter_for_404")
    def test_that_http_404_errors_are_not_retried(self, counter_for_404):
        # when
        self.execute_function_and_suppress_exceptions(
            TestGoogleHttpErrorRetryDecorator.TestClass().raise_HttpError404)
        # then
        self.assertEquals(1, counter_for_404.call_count)

    @patch.object(TestClass, "counter_for_500")
    def test_that_http_500_errors_are_retried(self, counter_for_500):
        # when
        self.execute_function_and_suppress_exceptions(
            TestGoogleHttpErrorRetryDecorator.TestClass().raise_HttpError500)
        # then
        self.assertEquals(3, counter_for_500.call_count)

    @classmethod
    def execute_function_and_suppress_exceptions(cls, function):
        try:
            function()
        except Exception:
            pass
