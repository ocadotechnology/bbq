import unittest
from datetime import datetime

from freezegun import freeze_time
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from tests.utils import backup_utils


class TestBackup(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def test_that_not_deleted_backup_doesnt_have_created_field(self):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        backup = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )
        backup.put()
        # then
        backup_to_check = Backup.get_by_key(backup.key)
        self.assertEqual(backup_to_check.deleted, None)
        self.assertEqual(
            backup_to_check.created, datetime(2017, 02, 1, 16, 30))

    def test_should_retrieve_table_using_backup(self):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        table.put()
        backup = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )
        backup.put()
        # then
        backup_entity = Backup.get_by_key(backup.key)
        table_entity = Table.get_table_from_backup(backup_entity)
        self.assertEqual(table_entity, table)

    @freeze_time("2017-02-03 16:30:00")
    def test_deleting_backup_is_adding_current_timestamp_in_deleted_field(
            self
        ):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        backup = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )
        backup.put()
        # when
        Backup.mark_backup_deleted(backup.key)
        # then
        deleted_backup = Backup.get_by_key(backup.key)
        self.assertEqual(deleted_backup.deleted, datetime(2017, 02, 3, 16, 30))

    def test_that_get_all_backups_sorted_will_return_only_these_with_null_deleted_column(self):# nopep8 pylint: disable=C0301, W0613
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        table.put()
        backup1 = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='backup_dataset',
            table_id='backup1',
            numBytes=1234,
        )
        backup1.put()
        backup2 = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='backup_dataset',
            table_id='backup2',
            numBytes=1234,
            deleted=datetime(2017, 02, 10, 16, 30)
        )
        backup2.put()
        # when
        existing_backups = Backup.get_all_backups_sorted(table.key)
        # then
        self.assertTrue(backup1 in existing_backups)
        self.assertTrue(backup2 not in existing_backups)

    def test_should_sort_backups_by_create_time_desc(self):
        # given
        b1 = backup_utils.create_backup(datetime(2017, 2, 3, 16, 30))
        b2 = backup_utils.create_backup(datetime(2017, 2, 2, 16, 30))
        b3 = backup_utils.create_backup(datetime(2017, 2, 1, 16, 30))
        expected_list = [b1, b2, b3]

        # when
        sorted_backups = Backup.sort_backups_by_create_time_desc([b1, b3, b2])

        # then
        self.assertListEqual(expected_list, sorted_backups)

    def test_should_not_sort_in_place(self):
        # given
        b1 = backup_utils.create_backup(datetime(2017, 2, 3, 16, 30))
        b2 = backup_utils.create_backup(datetime(2017, 2, 2, 16, 30))
        b3 = backup_utils.create_backup(datetime(2017, 2, 1, 16, 30))
        expected_list = [b1, b3, b2]
        actual_list = [b1, b3, b2]

        # when
        Backup.sort_backups_by_create_time_desc(actual_list)

        # then
        self.assertListEqual(expected_list, actual_list)

    def test_should_not_insert_two_backup_entities_for_the_same_backup_table(self):  # nopep8 pylint: disable=C0301
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        table.put()
        backup_one = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )
        backup_two = Backup(
            parent=table.key,
            last_modified=datetime(2018, 03, 2, 00, 00),
            created=datetime(2018, 03, 2, 00, 00),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )

        # when
        Backup.insert_if_absent(backup_one)
        Backup.insert_if_absent(backup_two)
        backups = list(Backup.get_all())

        # then
        self.assertEqual(len(backups), 1)
        self.assertEqual(backup_one.created, backups[0].created)

    def test_should_return_the_same_parent_table_for_child_backups(self):
        # given
        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        table.put()
        backup_one = Backup(
            parent=table.key,
            last_modified=datetime(2017, 02, 1, 16, 30),
            created=datetime(2017, 02, 1, 16, 30),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )
        backup_two = Backup(
            parent=table.key,
            last_modified=datetime(2018, 03, 2, 00, 00),
            created=datetime(2018, 03, 2, 00, 00),
            dataset_id='targetDatasetId',
            table_id='targetTableId',
            numBytes=1234
        )

        # when
        table1 = Backup.get_table(backup_one)
        table2 = Backup.get_table(backup_two)

        # then
        self.assertEqual(table, table1)
        self.assertEqual(table1, table2)