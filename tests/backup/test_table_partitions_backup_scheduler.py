import unittest

from google.appengine.ext import testbed, ndb
from mock import Mock, patch, call

from src.backup.table_partitions_backup_scheduler import \
    TablePartitionsBackupScheduler
from src.backup.task_creator import TaskCreator
from src.commons.table_reference import TableReference


class TestTablePartitionsBackupScheduler(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_taskqueue_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(TaskCreator, 'schedule_tasks_for_partition_backup')
    def test_schedule(self, schedule_tasks_for_partition_backup):
        # given
        project_id = "test-project"
        dataset_id = "test-dataset"
        table_id = "test-table"
        partition_id_1 = "20170330"
        partition_id_2 = "20170331"

        table_reference = TableReference(project_id=project_id,
                                         dataset_id=dataset_id,
                                         table_id=table_id,
                                         partition_id=None)

        big_query = Mock()

        # when
        big_query.list_table_partitions.return_value = [
            {"partitionId": partition_id_1},
            {"partitionId": partition_id_2}]

        TablePartitionsBackupScheduler(table_reference, big_query).start()

        # then
        schedule_tasks_for_partition_backup.assert_has_calls([
            call(project_id, dataset_id, table_id,
                 [partition_id_1, partition_id_2])
        ])
