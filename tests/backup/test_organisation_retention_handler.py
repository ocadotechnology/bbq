import datetime
import unittest

import webapp2
from google.appengine.ext import testbed

import webtest
from commons.test_utils import utils
from mock import patch
from src.backup.datastore.Table import Table
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
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    def test_should_schedul_retention_with_empty_datastore(self):
        # given
        # when
        self.under_test.get('/cron/retention')

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 0)

    def test_should_schedule_single_non_partitioned_table(self):
        # given
        self._create_table_entity('non_partitioned_table')

        # when
        self.under_test.get('/cron/retention')

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'
                         '&tableId=non_partitioned_table'
                         '&datasetId=example-dataset-name'
                         '&tableKey='),
                        msg='Actual url: {}'.format(tasks[0].url))

    def test_should_schedule_single_partitioned_table(self):
        # given
        self._create_table_entity('partitioned_table', '20170605')

        # when
        self.under_test.get('/cron/retention')

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'
                         '&partitionId=20170605'
                         '&tableId=partitioned_table'
                         '&datasetId=example-dataset-name'
                         '&tableKey='),
                        msg='Actual url: {}'.format(tasks[0].url))

    def test_should_schedule_using_cursor(self):
        # given
        self._create_table_entity('non_partitioned_table1')
        self._create_table_entity('non_partitioned_table2')

        _, cursor, _1 = Table.query().fetch_page(page_size=1)

        # when
        self.under_test.get(
            '/cron/retention?cursor={}'.format(cursor.urlsafe()))
        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'),
                        msg='Actual url: {}'.format(tasks[0].url))

    @patch('src.retention.organization_retention_handler.'
           'OrganizationRetentionHandler.QUERY_PAGE_SIZE', 3)
    def test_should_schedule_retention_task_at_the_end(self):
        # given
        for i in range(0, 6):
            self._create_table_entity('non_partitioned_table_{}'.format(i))

        # when
        self.under_test.get('/cron/retention')

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='table-retention-scheduler')

        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith('/cron/retention?cursor='),
                        msg='Actual url: {}'.format(tasks[0].url))

    @staticmethod
    def _create_table_entity(table_id, partition_id=None):
        non_partitioned_table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id=table_id,
            partition_id=partition_id,
            last_checked=datetime.datetime(2017, 02, 1, 16, 30)
        )
        non_partitioned_table.put()
