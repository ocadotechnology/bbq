import unittest

import webapp2
from google.appengine.ext import testbed

import webtest
from mock import patch
from src.retention.table_retention import TableRetention
from src.retention.table_retention_handler import TableRetentionHandler
from src.commons.table_reference import TableReference


class TestTableRetentionHandler(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        app = webapp2.WSGIApplication(
            [('/tasks/retention/table', TableRetentionHandler)])
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(TableRetention, 'perform_retention')
    def test_that_get_with_table_parameters_calls_method(
            self, perform_retention
    ):
        # given
        reference = TableReference('example-proj-name', 'example-dataset-name',
                       'example-table-name')


        # when
        self.under_test.get(
            '/tasks/retention/table',
            params={'projectId': 'example-proj-name',
                    'datasetId': 'example-dataset-name',
                    'tableId': 'example-table-name',
                    'tableKey': 'urlSafeKey'
                   }
        )

        # then
        perform_retention.assert_called_with(reference, 'urlSafeKey')

    @patch.object(TableRetention, 'perform_retention')
    def test_that_get_with_partition_parameters_calls_method(
            self, perform_retention
    ):
        # given
        reference = TableReference('example-proj-name', 'example-dataset-name',
                       'example-table-name', 'example-partition-id')

        # when
        self.under_test.get(
            '/tasks/retention/table',
            params={'projectId': 'example-proj-name',
                    'datasetId': 'example-dataset-name',
                    'tableId': 'example-table-name',
                    'partitionId': 'example-partition-id',
                    'tableKey': 'urlSafeKey'
                   }
        )

        # then
        perform_retention.assert_called_with(reference, 'urlSafeKey')
