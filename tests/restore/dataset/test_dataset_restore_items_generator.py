from datetime import datetime
from unittest import TestCase

from freezegun import freeze_time
from google.appengine.ext import testbed, ndb
from mock import patch, PropertyMock

from src.backup.datastore.Table import Table
from src.commons.config.configuration import Configuration
from src.restore.dataset.dataset_restore_items_generator import \
    DatasetRestoreItemsGenerator
from src.restore.datastore.restore_item import RestoreItem
from src.commons.table_reference import TableReference
from tests.utils import table_entities_creator

RESTORATION_JOB_ID = 'restoration_job_id'

BACKUP_PROJECT_ID = 'backup_project_id'
RESTORATION_PROJECT_ID = 'restoration_project_id'

PROJECT_TO_RESTORE = 'project-x'
DATASET_TO_RESTORE = 'dataset_y'


class TestDatasetRestoreItemsGenerator(TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_taskqueue_stub()
        ndb.get_context().clear_cache()

        patch.object(Configuration, 'backup_project_id',
                     return_value=BACKUP_PROJECT_ID,
                     new_callable=PropertyMock).start()

        self.freezer = freeze_time("2017-12-06")
        self.freezer.start()

    def tearDown(self):
        patch.stopall()
        self.freezer.stop()
        self.testbed.deactivate()

    def test_should_create_restore_items_when_no_target_dataset(self):
        # given
        restore_item = self.__prepare_entities_and_create_expected_restore_item()

        # when
        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id=RESTORATION_PROJECT_ID,
                                       target_dataset_id=self.__create_target_dataset(
                                           None),
                                       max_partition_days=None)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item]])

    def test_should_create_restore_items_when_target_project_id(self):
        # given
        restore_item = self.__prepare_entities_and_create_expected_restore_item(target_project_id="TARGET-PROJECT")


        # when
        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id="TARGET-PROJECT",
                                       target_dataset_id=self.__create_target_dataset(
                                           None),
                                       max_partition_days=None)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item]])

    def test_should_create_restore_items_target_dataset_provided(
            self):
        custom_target_dataset_id = "custom_target_dataset_id"
        restore_item = self.__prepare_entities_and_create_expected_restore_item(
            target_dataset=custom_target_dataset_id)

        # when
        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id=RESTORATION_PROJECT_ID,
                                       target_dataset_id=custom_target_dataset_id,
                                       max_partition_days=None)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item]])

    def test_should_not_create_restore_items_for_deleted_backups(self):
        # given
        self.__prepare_entities_with_removed_backup()
        restore_item = self.__prepare_entities_and_create_expected_restore_item()

        # when
        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id=RESTORATION_PROJECT_ID,
                                       target_dataset_id=self.__create_target_dataset(
                                           None),
                                       max_partition_days=None)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item]])

    def test_should_create_restore_item_for_partition(
            self):
        # given
        restore_item = self.__prepare_entities_and_create_expected_restore_item(
            partition_id='20171205')

        # when
        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id=RESTORATION_PROJECT_ID,
                                       target_dataset_id=self.__create_target_dataset(
                                           None),
                                       max_partition_days=None)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item]])

    def test_service_should_generate_restore_correct_partitioned_items(self):
        # given
        non_part_tab, tab_part1, tab_part2 = \
            self.__prepare_tables_which_should_be_returned_by_max_partition_days_query()
        self.__prepare_tables_which_should_not_be_returned_by_max_partition_days_query()

        restore_item_1 = self.__generate_expected_restore_item(tab_part1)
        restore_item_2 = self.__generate_expected_restore_item(tab_part2)
        restore_item_3 = self.__generate_expected_restore_item(non_part_tab)

        generated_restore_items = [i for i in
                                   DatasetRestoreItemsGenerator.generate_restore_items(
                                       project_id=PROJECT_TO_RESTORE,
                                       dataset_id=DATASET_TO_RESTORE,
                                       target_project_id=RESTORATION_PROJECT_ID,
                                       target_dataset_id=self.__create_target_dataset(
                                           None),
                                       max_partition_days=2)]
        # then
        self.assertEqual(generated_restore_items, [[restore_item_1,
                                                    restore_item_2,
                                                    restore_item_3]])

    @staticmethod
    def __prepare_entities_and_create_expected_restore_item(partition_id=None,
                                                            target_project_id=RESTORATION_PROJECT_ID,
                                                            target_dataset=None):
        table = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl1',
            date=datetime(2017, 12, 5),
            partition_id=partition_id)

        return TestDatasetRestoreItemsGenerator.__generate_expected_restore_item(
            table=table,
            target_project_id=target_project_id,
            custom_target_dataset=target_dataset)

    @staticmethod
    def __prepare_entities_with_removed_backup():
        table = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl1',
            date=datetime(2017, 12, 5),
            partition_id=None)
        table.last_backup.deleted = datetime.now()
        table.last_backup.put()

    @staticmethod
    def __create_table_without_backup(project_id, dataset_id):
        table_without_backup = Table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id='table_id_without_backup',
            partition_id=None,
            last_checked=datetime.now()
        )
        table_without_backup.put()

    @staticmethod
    def __prepare_tables_which_should_be_returned_by_max_partition_days_query():
        tab_with_partition1 = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl1',
            date=datetime(2017, 12, 6),
            partition_id='20171206'
        )
        tab_with_partition2 = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl1',
            date=datetime(2017, 12, 5),
            partition_id='20171205'
        )
        non_partitioned_table = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl3',
            date=datetime(2017, 11, 5)
        )

        return non_partitioned_table, tab_with_partition1, tab_with_partition2

    @staticmethod
    def __prepare_tables_which_should_not_be_returned_by_max_partition_days_query():
        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl1',
            date=datetime(2017, 12, 3),  # too old partition (max_days > 2)
            partition_id='20171203')
        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id='ANOTHER_DATASET',
            table_id='tbl3',
            date=datetime(2017, 12, 5))
        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id='ANOTHER_PROJECT',
            dataset_id=DATASET_TO_RESTORE,
            table_id='tbl3',
            date=datetime(2017, 12, 5))
        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE,
            dataset_id='ANOTHER_DATASET',
            table_id='tbl1',
            date=datetime(2017, 12, 4),
            partition_id='20171204')
        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id='source_proj',
            dataset_id='dataset2',
            table_id='tbl1',
            date=datetime(2017, 12, 4),
            partition_id='20171201')

    @staticmethod
    def __generate_expected_restore_item(
            table,
            target_project_id=RESTORATION_PROJECT_ID,
            custom_target_dataset=None):
        expected_source = TableReference(project_id=BACKUP_PROJECT_ID,
                                         dataset_id=table.last_backup.dataset_id,
                                         table_id=table.last_backup.table_id,
                                         partition_id=table.partition_id)

        target_dataset = TestDatasetRestoreItemsGenerator.__create_target_dataset(
            custom_target_dataset)
        expected_target = TableReference(project_id=target_project_id,
                                         dataset_id=target_dataset,
                                         table_id=table.table_id,
                                         partition_id=table.partition_id)
        expected_restore_item = RestoreItem.create(expected_source,
                                                   expected_target)
        return expected_restore_item

    @staticmethod
    def __create_target_dataset(custom_target_dataset):
        if custom_target_dataset is not None:
            return custom_target_dataset
        return DATASET_TO_RESTORE
