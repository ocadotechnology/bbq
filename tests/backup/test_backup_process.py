import datetime
import unittest

from freezegun import freeze_time
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mock import patch, Mock, MagicMock

from src.backup.backup_process import BackupProcess
from src.backup.dataset_id_creator import DatasetIdCreator
from src.backup.datastore.Table import Table
from src.backup.default_backup_predicate import DefaultBackupPredicate
from src.backup.backup_creator import BackupCreator
from src.backup.on_demand.on_demand_backup_predicate import \
  OnDemandBackupPredicate
from src.commons.table_reference import TableReference

copy_job_constructor = 'src.backup.backup_creator.BackupCreator.__init__'


@freeze_time("2017-04-04")
@patch(copy_job_constructor, Mock(return_value=None))
class TestBackupProcess(unittest.TestCase):

    def setUp(self):
        self.init_test_bed_for_datastore()
        self.big_query = Mock()
        self.big_query_table_metadata = Mock()

    def init_test_bed_for_datastore(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_taskqueue_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(DefaultBackupPredicate, 'test', return_value=True)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    def test_that_copy_job_and_entity_in_datastore_is_created_if_empty_partitioned_table( # nopep8 pylint: disable=C0301
            self, create_backup, _, _1):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id=None)

        # when
        BackupProcess(table_reference, self.big_query,
                      self.big_query_table_metadata).start()
        table_in_datastore = Table.get_table("test-project", "test-dataset",
                                             "test-table")

        # then
        create_backup.assert_called_once()
        self.assertIsNotNone(table_in_datastore)

    @patch.object(DefaultBackupPredicate, 'test', return_value=True)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    def test_copy_job_and_entity_in_datastore_for_single_partition_of_a_table(
            self, _, _1, _2):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table",
                                         partition_id="20170330")

        # when
        BackupProcess(table_reference, self.big_query,
                      self.big_query_table_metadata).start()

        ancestor_of_partition = Table.get_table("test-project", "test-dataset",
                                                "test-table")
        partition = Table.get_table("test-project", "test-dataset",
                                    "test-table", "20170330")

        # then
        self.assertIsNotNone(partition)
        self.assertIsNone(ancestor_of_partition)

    @patch.object(DefaultBackupPredicate, 'test', return_value=True)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    def test_copy_job_and_entity_in_datastore_for_not_partitioned_table(
            self, _, _1, _2):
        # given
        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table")

        # when
        BackupProcess(table_reference, self.big_query,
                      self.big_query_table_metadata).start()

        table_entity = Table.get_table("test-project", "test-dataset",
                                       "test-table")

        # then
        self.assertIsNotNone(table_entity)

    @patch.object(DefaultBackupPredicate, 'test', return_value=True)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    def test_that_last_checked_date_is_updated_when_backup_is_processed(
            self, _, _1, _2):
        # given
        table = Table(project_id="test-project",
                      dataset_id="test-dataset",
                      table_id="test-table",
                      last_checked=datetime.datetime(2017, 3, 3))

        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table")

        # when
        table.put()

        BackupProcess(table_reference, self.big_query,
                      self.big_query_table_metadata).start()

        table_entity = Table.get_table("test-project", "test-dataset",
                                       "test-table")

        # then
        self.assertEqual(table_entity.last_checked,
                         datetime.datetime(2017, 04, 4))

    @patch.object(DefaultBackupPredicate, 'test', return_value=False)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    def test_that_last_checked_date_is_updated_even_if_table_should_not_be_backed_up( # nopep8 pylint: disable=C0301
            self, copy_table, _1, _2):
        # given
        table = Table(project_id="test-project",
                      dataset_id="test-dataset",
                      table_id="test-table",
                      last_checked=datetime.datetime(2017, 3, 3))

        table_reference = TableReference(project_id="test-project",
                                         dataset_id="test-dataset",
                                         table_id="test-table")

        # when
        table.put()

        BackupProcess(table_reference, self.big_query,
                      self.big_query_table_metadata).start()

        table_entity = Table.get_table("test-project", "test-dataset",
                                       "test-table")

        # then
        self.assertEqual(table_entity.last_checked,
                         datetime.datetime(2017, 04, 4))
        copy_table.assert_not_called()

    @patch.object(DefaultBackupPredicate, 'test', return_value=True)
    @patch.object(DatasetIdCreator, 'create', return_value='target_dataset_id')
    @patch.object(BackupCreator, 'create_backup')
    @patch.object(Table, "get_table")
    def test_that_dataset_will_not_be_unnecessary_created_twice(self,
                                                                _, _1, _2, _3):
        # given
        table_reference_1 = TableReference(project_id="test-project",
                                           dataset_id="test-dataset",
                                           table_id="test-table-1")
        table_reference_2 = TableReference(project_id="test-project",
                                           dataset_id="test-dataset",
                                           table_id="test-table-2")

        # when
        self.big_query.create_dataset = MagicMock()

        BackupProcess(table_reference_1, self.big_query,
                      self.big_query_table_metadata).start()
        BackupProcess(table_reference_2, self.big_query,
                      self.big_query_table_metadata).start()

        # then
        self.big_query.create_dataset.assert_called_once()
