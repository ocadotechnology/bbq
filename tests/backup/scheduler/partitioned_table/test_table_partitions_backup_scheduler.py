import os
import unittest

from google.appengine.ext import testbed, ndb
from mock import patch, Mock

from src.backup.scheduler.partitioned_table.partitioned_table_backup_scheduler import \
    PartitionedTableBackupScheduler
from src.commons import request_correlation_id
from src.commons.test_utils import utils


class TestPartitionedTableBackupScheduler(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.task_queue_stub = utils.init_testbed_queue_stub(self.testbed)
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_table_partitions')
    def test_partitioned_table_backup_scheduler_should_create_backup_tasks(
        self, list_table_partitions, _1):
        # given
        partition_id_1 = "20170330"
        partition_id_2 = "20170331"
        list_table_partitions.return_value = [
            {"partitionId": partition_id_1},
            {"partitionId": partition_id_2}]

        # when
        PartitionedTableBackupScheduler().schedule_backup(
            project_id='test-project-id',
            dataset_id='dataset_1', table_id='table_id')

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-worker')

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url,
                         '/tasks/backups/table/test-project-id/dataset_1/table_id/' + partition_id_1)
        self.assertEqual(tasks[1].url,
                         '/tasks/backups/table/test-project-id/dataset_1/table_id/' + partition_id_2)
