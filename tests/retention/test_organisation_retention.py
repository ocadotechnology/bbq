import datetime
import unittest

from dateutil.relativedelta import relativedelta
from google.appengine.ext import testbed
from mock import patch

from src.backup.datastore.Table import Table
from src.commons.config.configuration import configuration
from src.commons.test_utils import utils
from src.retention.organization_retention import OrganizationRetention


class TestOrganizationRetention(unittest.TestCase):

    def setUp(self):

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()
        self.taskqueue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    def test_should_schedule_retention_with_empty_datastore(self):
        # given
        # when
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(None)

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 0)

    def test_should_schedule_single_non_partitioned_table(self):
        # given
        self._create_table_entity('non_partitioned_table')

        # when
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(None)

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
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(None)

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

    def test_should_schedule_only_recently_seen_tables(self):
        # given
        self._create_table_entity('recently_seen_partitioned_table', '20170605')
        self._create_table_entity('recently_seen_not_partitioned_table')
        self._create_table_entity('not_seen_since_threshold_date_table', last_checked= datetime.datetime.now() - relativedelta(
            months=(configuration.grace_period_after_source_table_deletion_in_months + 2)))

        # when
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(None)

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 2)
        self.assertTrue(tasks[0].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'
                         '&partitionId=20170605'
                         '&tableId=recently_seen_partitioned_table'
                         '&datasetId=example-dataset-name'
                         '&tableKey='),
                        msg='Actual url: {}'.format(tasks[0].url))
        self.assertTrue(tasks[1].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'
                         '&tableId=recently_seen_not_partitioned_table'
                         '&datasetId=example-dataset-name'
                         '&tableKey='),
                        msg='Actual url: {}'.format(tasks[1].url))

    def test_should_schedule_using_cursor(self):
        # given
        self._create_table_entity('non_partitioned_table1')
        self._create_table_entity('non_partitioned_table2')

        age_threshold_datetime = datetime.datetime.today() - relativedelta(
            months=(configuration.grace_period_after_source_table_deletion_in_months + 1))

        _, cursor, _1 = Table.query() \
            .filter(Table.last_checked >= age_threshold_datetime) \
            .order(Table.last_checked, Table.key) \
            .fetch_page(
            page_size=1,
        )

        # when
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(cursor)
        # then
        tasks = self.taskqueue_stub.get_filtered_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith
                        ('/tasks/retention/table'
                         '?projectId=example-proj-name'),
                        msg='Actual url: {}'.format(tasks[0].url))

    @patch('src.retention.organization_retention_handler.'
           'OrganizationRetention.QUERY_PAGE_SIZE', 3)
    def test_should_schedule_retention_task_at_the_end(self):
        # given
        for i in range(0, 6):
            self._create_table_entity('non_partitioned_table_{}'.format(i))

        # when
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(None)

        # then
        tasks = self.taskqueue_stub.get_filtered_tasks(
            queue_names='table-retention-scheduler')

        self.assertEqual(len(tasks), 1)
        self.assertTrue(tasks[0].url.startswith('/cron/retention?cursor='),
                        msg='Actual url: {}'.format(tasks[0].url))

    @staticmethod
    def _create_table_entity(table_id, partition_id=None, last_checked=datetime.datetime.now()):
        non_partitioned_table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id=table_id,
            partition_id=partition_id,
            last_checked=last_checked
        )
        non_partitioned_table.put()
