import datetime
import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mock import patch, call, PropertyMock

from src.commons.config.configuration import Configuration
from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.big_query.big_query import BigQuery, TableNotFoundException
from src.retention.policy.delete_non_partitioned_tables_above_10_versions import \
    DeleteNonPartitionedTablesOlderThan10Versions
from src.retention.table_retention import TableRetention
from src.commons.table_reference import TableReference
from tests.utils import backup_utils


class TestTableRetention(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self.init_testbed_for_datastore()
        self.under_test = TableRetention(
            DeleteNonPartitionedTablesOlderThan10Versions())

    def init_testbed_for_datastore(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(BigQuery, 'delete_table', side_effect=[TableNotFoundException('Table Not Found'), None])  # nopep8 pylint: disable=C0301
    @patch("src.retention.policy.delete_non_partitioned_tables_above_10_versions.DeleteNonPartitionedTablesOlderThan10Versions.NUMBER_OF_BACKUPS_TO_KEEP", 0) # nopep8 pylint: disable=C0301
    @patch.object(Configuration, 'backup_project_id', return_value='dummy_project_id', new_callable=PropertyMock)
    def test_that_None_in_deleted_attribute_should_be_updated_with_a_date_when_table_not_found(self, _, _1): # nopep8 pylint: disable=C0301,W0613
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])

        reference = TableReference.from_table_entity(table)

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())
        # then
        self.assertTrue(
            Backup.get_by_key(backup1.key).deleted is not None)
        self.assertTrue(
            Backup.get_by_key(backup2.key).deleted is not None)

    @patch.object(BigQuery, 'delete_table', side_effect=[TableNotFoundException('Table Not Found'), None]) # nopep8 pylint: disable=C0301
    @patch("src.retention.policy.delete_non_partitioned_tables_above_10_versions.DeleteNonPartitionedTablesOlderThan10Versions.NUMBER_OF_BACKUPS_TO_KEEP", 0) # nopep8 pylint: disable=C0301
    @patch.object(Configuration, 'backup_project_id', return_value='dummy_project_id', new_callable=PropertyMock)
    def test_that_if_first_backup_of_a_table_not_found_the_next_one_will_proceed(self, _, delete_table): # nopep8 pylint: disable=C0301, W0613, R0201
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])
        reference = TableReference.from_table_entity(table)

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())
        # then
        delete_table.assert_has_calls([
            call(TableReference('dummy_project_id', 'targetDataset', 'backup2')),
            call(TableReference('dummy_project_id', 'targetDataset', 'backup1'))
        ])
