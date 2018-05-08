import unittest

from freezegun import freeze_time
from google.appengine.ext import testbed, ndb
from mock import patch, Mock

from src.backup.backup_process import BackupProcess
from src.backup.table_backup import TableBackup
from src.backup.table_partitions_backup_scheduler import \
    TablePartitionsBackupScheduler
from src.table_reference import TableReference


@freeze_time("2017-04-04")
@patch('src.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
class TestTableBackup(unittest.TestCase):

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

    @patch('src.big_query.big_query.BigQuery.get_table_or_partition.return_value.is_daily_partitioned.return_value', True)  # nopep8 pylint: disable=C0301
    @patch('src.big_query.big_query.BigQuery.get_table_or_partition.return_value.is_empty.return_value', False)  # nopep8 pylint: disable=C0301
    @patch('src.big_query.big_query.BigQuery.get_table_or_partition')
    @patch.object(TablePartitionsBackupScheduler, 'start')
    def test_that_partition_backups_are_scheduled_for_partitioned_table(
            self, _, table_partitions_backup_scheduler):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id="20170303")

        # when
        TableBackup.start(table_reference)

        # then
        table_partitions_backup_scheduler.assert_called_once()

    @patch('src.big_query.big_query.BigQuery.get_table_or_partition.return_value.is_daily_partitioned.return_value', False)  # nopep8 pylint: disable=C0301
    @patch('src.big_query.big_query.BigQuery.get_table_or_partition')
    @patch.object(BackupProcess, 'start')
    def test_that_backup_is_created_for_not_partitioned_table(
            self, backup_start, _):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id=None)

        # when
        TableBackup.start(table_reference)

        # then
        backup_start.assert_called_once()
