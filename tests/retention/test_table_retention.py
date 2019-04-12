import datetime
import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mock import patch, Mock

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.big_query.big_query import BigQuery, TableNotFoundException
from src.commons.table_reference import TableReference
from src.retention.table_retention import TableRetention
from tests.utils import backup_utils


class TestTableRetention(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self.init_testbed_for_datastore()
        self.policy = Mock()
        self.under_test = TableRetention(self.policy)

    def init_testbed_for_datastore(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(BigQuery, 'delete_table')
    def test_should_fill_deleted_field_in_backup_entity_after_deletion(self, _):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        reference = TableReference.from_table_entity(table)
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])
        self.policy.get_backups_eligible_for_deletion = Mock(
            return_value=[backup1, backup2])

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())

        # then
        self.assertTrue(
            Backup.get_by_key(backup1.key).deleted is not None)
        self.assertTrue(
            Backup.get_by_key(backup2.key).deleted is not None)

    @patch.object(BigQuery, 'delete_table')
    def test_should_not_delete_any_table_if_policy_return_empty_list(self, delete_table):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        reference = TableReference.from_table_entity(table)
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])
        self.policy.get_backups_eligible_for_deletion = Mock(return_value=[])

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())

        # then
        self.assertTrue(
            Backup.get_by_key(backup1.key).deleted is None)
        self.assertTrue(
            Backup.get_by_key(backup2.key).deleted is None)
        delete_table.assert_not_called()

    @patch.object(BigQuery, 'delete_table',
                  side_effect=[TableNotFoundException('Table Not Found'), None])
    def test_should_fill_deleted_field_in_backup_entity_if_table_not_found_error_during_deletion(
            self, _):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        reference = TableReference.from_table_entity(table)
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])
        self.policy.get_backups_eligible_for_deletion = Mock(
            return_value=[backup1, backup2])

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())

        # then
        self.assertTrue(
            Backup.get_by_key(backup1.key).deleted is not None)
        self.assertTrue(
            Backup.get_by_key(backup2.key).deleted is not None)

    @patch.object(BigQuery, 'delete_table')
    def test_should_fill_deleted_field_in_backup_entity_only_for_deleted_backup(
            self, _):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        reference = TableReference.from_table_entity(table)
        backup1 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 1, 16, 30), table, table_id="backup1")
        backup2 = backup_utils.create_backup(
            datetime.datetime(2017, 2, 2, 16, 30), table, table_id="backup2")
        ndb.put_multi([backup1, backup2])
        self.policy.get_backups_eligible_for_deletion = Mock(
            return_value=[backup1])

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())

        # then
        self.assertTrue(
            Backup.get_by_key(backup1.key).deleted is not None)
        self.assertTrue(
            Backup.get_by_key(backup2.key).deleted is None)

    @patch.object(BigQuery, 'delete_table')
    def test_should_not_perform_retention_if_no_backups(self, delete_table):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime.datetime(2017, 2, 1, 16, 30)
        )
        table.put()
        reference = TableReference.from_table_entity(table)

        # when
        self.under_test.perform_retention(reference, table.key.urlsafe())

        # then
        delete_table.assert_not_called()
