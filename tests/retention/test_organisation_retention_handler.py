import unittest

import webapp2
import webtest
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import testbed
from mock import patch

from src.retention.organization_retention_handler import \
    OrganizationRetentionHandler


class TestOrganizationRetentionHandler(unittest.TestCase):

    def setUp(self):
        app = webapp2.WSGIApplication(
            [('/cron/retention', OrganizationRetentionHandler)])
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

    def tearDown(self):
        self.testbed.deactivate()

    @patch('src.retention.organization_retention_handler.OrganizationRetention'
           '.schedule_retention_tasks_starting_from_cursor')
    def test_should_call_organisation_retention(self, schedule_retention_tasks_starting_from_cursor):
        # given
        # when
        self.under_test.get('/cron/retention')

        # then
        schedule_retention_tasks_starting_from_cursor.assert_called_with(Cursor(urlsafe=None))

    @patch('src.retention.organization_retention_handler.OrganizationRetention'
           '.schedule_retention_tasks_starting_from_cursor')
    def test_should_call_organisation_retention_with_cursor(self, schedule_retention_tasks_starting_from_cursor):
        # given
        urlsafe_cursor_example = "<CjwKGQoMbGFzdF9jaGVja2VkEgkI-87x367N4gISG2oMdGVzdGJlZC10ZXN0cgsLEgVUYWJsZRgBDBgAIAA=>"

        # when
        self.under_test.get(
            '/cron/retention?cursor={}'.format(urlsafe_cursor_example))

        # then
        schedule_retention_tasks_starting_from_cursor.assert_called_with(Cursor(urlsafe=urlsafe_cursor_example))
