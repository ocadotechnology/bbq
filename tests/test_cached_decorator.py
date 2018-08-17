import unittest

from google.appengine.api import memcache
from google.appengine.ext import testbed

from src.commons.decorators.cached import cached
from mock import patch, call

RETURNED_VALUE = 'importantString'


class TestCachedDecorator(unittest.TestCase):
    class TestClass(object):

        @cached(time=10)
        def return_important_string(self):
            result = self.do_big_computation()
            return result

        def do_big_computation(self):
            return RETURNED_VALUE

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(TestClass, 'do_big_computation', return_value=RETURNED_VALUE)
    def test_that_cached_let_the_function_be_executed_only_first_time(
            self, do_big_computation):
        test_class = TestCachedDecorator.TestClass()

        # when
        result1 = test_class.return_important_string()
        result2 = test_class.return_important_string()
        result3 = test_class.return_important_string()
        result4 = test_class.return_important_string()

        # then
        do_big_computation.assert_called_once()
        self.assertEquals(RETURNED_VALUE, result1)
        self.assertEquals(RETURNED_VALUE, result2)
        self.assertEquals(RETURNED_VALUE, result3)
        self.assertEquals(RETURNED_VALUE, result4)

    @patch.object(TestClass, 'do_big_computation', return_value=RETURNED_VALUE)
    def test_that_cached_function_will_be_executed_after_invalidating_cache(
            self, do_big_computation):
        test_class = TestCachedDecorator.TestClass()

        # when
        result1 = test_class.return_important_string()
        result2 = test_class.return_important_string()
        result3 = test_class.return_important_string()

        memcache.flush_all()

        result4 = test_class.return_important_string()

        # then
        do_big_computation.assert_has_calls([call(), call()])
        self.assertEquals(RETURNED_VALUE, result1)
        self.assertEquals(RETURNED_VALUE, result2)
        self.assertEquals(RETURNED_VALUE, result3)
        self.assertEquals(RETURNED_VALUE, result4)

    @patch.object(TestClass, 'do_big_computation', return_value=None)
    def test_function_should_be_executed_each_time_if_return_none_value(
            self, do_big_computation):
        test_class = TestCachedDecorator.TestClass()

        # when
        result1 = test_class.return_important_string()
        result2 = test_class.return_important_string()
        result3 = test_class.return_important_string()

        # then
        do_big_computation.assert_has_calls([call(), call(), call()])
        self.assertEquals(None, result1)
        self.assertEquals(None, result2)
        self.assertEquals(None, result3)
