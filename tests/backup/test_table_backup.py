import unittest

from freezegun import freeze_time
from google.appengine.ext import testbed, ndb
from mock import patch, Mock

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.backup.backup_process import BackupProcess
from src.backup.table_backup import TableBackup
from src.backup.table_partitions_backup_scheduler import \
    TablePartitionsBackupScheduler
from src.commons.table_reference import TableReference


@freeze_time("2017-04-04")
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

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BackupProcess, 'start')
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'is_daily_partitioned', return_value=True)
    @patch.object(BigQueryTableMetadata, 'is_partition', return_value=True)
    @patch.object(BigQueryTableMetadata, 'is_empty', return_value=False)
    def test_that_backup_are_scheduled_for_non_empty_single_partition(
            self, _, _1, _2,_3, backup_start):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id="20170303")

        # when
        TableBackup.start(table_reference)

        # then
        backup_start.assert_called_once()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'is_daily_partitioned', return_value=False)
    @patch.object(BackupProcess, 'start')
    def test_that_table_backup_is_scheduled_for_not_partitioned_table(
            self, backup_start, _, _1):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id=None)
        # when
        TableBackup.start(table_reference)

        # then
        backup_start.assert_called_once()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'is_daily_partitioned', return_value=True)
    @patch.object(BigQueryTableMetadata, 'is_partition', return_value=False)
    @patch.object(BigQueryTableMetadata, 'is_empty', return_value=False)
    @patch.object(TablePartitionsBackupScheduler, 'start')
    def test_that_backup_for_partitions_is_scheduled_for_partitioned_table(
        self, table_partitions_backup_scheduler, _, _1, _2,_3):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id=None)

        # when
        TableBackup.start(table_reference)

        # then
        table_partitions_backup_scheduler.assert_called_once()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'is_daily_partitioned', return_value=True)
    @patch.object(BigQueryTableMetadata, 'is_partition', return_value=False)
    @patch.object(BigQueryTableMetadata, 'is_empty', return_value=True)
    @patch.object(TablePartitionsBackupScheduler, 'start')
    def test_that_backup_for_partitions_is_scheduled_for_empty_partitioned_table(
            self, table_partitions_backup_scheduler, _, _1, _2,_3):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id=None)

        # when
        TableBackup.start(table_reference)

        # then
        table_partitions_backup_scheduler.assert_called_once()
