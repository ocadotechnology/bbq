import datetime
import unittest

from google.appengine.ext import testbed, ndb

from src.commons.exceptions import NotFoundException
from src.backup.datastore.Table import Table
from src.backup.datastore.backup_finder import BackupFinder
from src.commons.table_reference import TableReference
from tests.utils import backup_utils

PROJECT_ID = 'project-id'
DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'
PARTITION_ID = 'partitionId'
PARTITION_ID_WITHOUT_BACKUP = "partitionIdWithoutBackup"

NOW = datetime.datetime(2017, 03, 01)
OLD_TIME = datetime.datetime(2016, 03, 01)

BACKUP_TABLE_ID_FROM_NOW = TABLE_ID + NOW.strftime('%Y%m%d')
BACKUP_TABLE_ID_FROM_OLD_TIME = TABLE_ID + OLD_TIME.strftime('%Y%m%d')

TABLE_REFERENCE = TableReference(PROJECT_ID, DATASET_ID, TABLE_ID, PARTITION_ID)
TABLE_REFERENCE_WITHOUT_BACKUP = \
    TableReference(PROJECT_ID, DATASET_ID, TABLE_ID, PARTITION_ID_WITHOUT_BACKUP)


class TestBackupFinder(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        self.__create_table_with_two_backups()
        self.__create_table_without_backups()

    def tearDown(self):
        self.testbed.deactivate()

    def test_return_latest_backup_for_now(self):
        # given && when
        result = BackupFinder.for_table(TABLE_REFERENCE, NOW)

        # then
        self.assertEquals(BACKUP_TABLE_ID_FROM_NOW, result.table_id)

    def test_return_latest_backup_if_none_datetime_given(self):
        # given && when
        result = BackupFinder.for_table(TABLE_REFERENCE, None)

        # then
        self.assertEquals(BACKUP_TABLE_ID_FROM_NOW, result.table_id)

    def test_return_old_backup_for_older_datetime_then_now(self):
        # given
        older_datetime = NOW - datetime.timedelta(days=1)

        # when
        result = BackupFinder.for_table(TABLE_REFERENCE, older_datetime)

        # then
        self.assertEquals(BACKUP_TABLE_ID_FROM_OLD_TIME, result.table_id)

    def test_throw_error_if_table_not_found(self):
        # given
        not_existing_table_reference = \
            TableReference(PROJECT_ID, DATASET_ID, TABLE_ID, "partitionId2")
        error_message = 'Table not found in datastore: {}'\
            .format(not_existing_table_reference)

        # when
        with self.assertRaises(NotFoundException) as context:
            BackupFinder.for_table(not_existing_table_reference, NOW)

        # then
        self.assertTrue(error_message in context.exception)

    def test_throw_error_if_backup_not_found(self):
        # given
        error_message = 'Backup not found for {} before {}'\
            .format(TABLE_REFERENCE_WITHOUT_BACKUP, NOW)

        # when
        with self.assertRaises(NotFoundException) as context:
            BackupFinder.for_table(TABLE_REFERENCE_WITHOUT_BACKUP, NOW)

        # then
        self.assertTrue(error_message in context.exception)

    @staticmethod
    def __create_table_with_two_backups():
        table = Table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partition_id=PARTITION_ID,
            last_checked=NOW
        )
        table.put()

        backup_utils.create_backup(NOW, table, BACKUP_TABLE_ID_FROM_NOW).put()
        backup_utils.create_backup(OLD_TIME, table,
                                   BACKUP_TABLE_ID_FROM_OLD_TIME).put()

    @staticmethod
    def __create_table_without_backups():
        partition_id = "partitionIdWithoutBackup"
        table = Table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partition_id=partition_id,
            last_checked=NOW
        )
        table.put()
