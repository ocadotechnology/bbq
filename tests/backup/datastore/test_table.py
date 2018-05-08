from datetime import datetime
from unittest import TestCase

from freezegun import freeze_time
from google.appengine.ext import testbed, ndb

from src.backup.datastore.Table import Table
from tests.utils import backup_utils, table_entities_creator


class TestTable(TestCase):

    def setUp(self):
        self.init_testbed_for_datastore()

    def init_testbed_for_datastore(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def test_last_backup_property(self):
        # given
        table = self.__create_example_table()

        with freeze_time("2012-01-14") as frozen_datetime:
            backup_utils.create_backup(datetime.utcnow(), table,
                                       "first_backup").put()
            frozen_datetime.move_to("2012-01-15")
            backup_utils.create_backup(datetime.utcnow(), table,
                                       "second_backup").put()

        # when
        last_backup = table.last_backup

        # then
        self.assertEqual(last_backup.table_id, "second_backup")
        self.assertEqual(last_backup.created, datetime(2012, 1, 15))

    def __create_example_table(self):
        project_id = 'proj1'
        dataset_id = 'dataset1'
        table_id = 'tbl1'
        return self.__create_single_table(project_id, dataset_id, table_id)

    def create_table_with(self, partition_id):
        table = Table(project_id='p1', dataset_id='d1', table_id='t1',
                      partition_id=partition_id)
        table.put()
        return table

    def __create_single_table(self, project_id, dataset_id, table_id, partition_id=None):
        table = Table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            partition_id=partition_id
        )
        table.put()
        return table

    def test_get_last_backup_for_tables(self):
        # given
        table1, table2 = self.__create_two_tables_with_single_backups()

        # when
        last_backups_list = Table.get_last_backup_for_tables([table1, table2])

        # then
        self.assertEqual(len(list(last_backups_list)), 2)

    def __create_two_tables_with_single_backups(self):
        table1 = table_entities_creator.create_and_insert_table_with_one_backup(
            'proj1',
            'dataset1',
            'tbl1',
            datetime(
                2017,
                12, 5))
        table2 = table_entities_creator.create_and_insert_table_with_one_backup(
            'proj1',
            'dataset1',
            'tbl2',
            datetime(
                2017,
                12, 4))
        return table1, table2

    def test_should_return_last_backup_not_newer_than(self):
        # given
        backup_day_1 = datetime(2017, 02, 3)
        backup_day_4 = datetime(2017, 02, 6)
        backup_day_5 = datetime(2017, 02, 7)

        table = Table(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            table_id='example-table-name',
            last_checked=datetime(2017, 02, 1, 16, 30)
        )
        table.put()

        backups = backup_utils \
            .create_backup_daily_sequence(4, table, backup_day_1)
        for backup in backups:
            backup.put()

        # when
        returned_last_backup = table.last_backup_not_newer_than(backup_day_5)

        # then
        self.assertEqual(returned_last_backup.created, backup_day_4)

    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__less_than_0(self):
        #given
        table2 = self.create_table_with(partition_id='20180214')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=-1))

        #then
        self.assertEqual(len(tables), 0)

    def test_should_return_tables_from_above_first_page(
            self):
        # given
        table_entities_creator.create_multiple_table_entities(quantity=3, project_id='example-proj-name', partition_id=None)

        # when
        page_size = 1
        tables = list(Table.get_tables_with_max_partition_days(
            project_id='example-proj-name',
            dataset_id='example-dataset-name',
            max_partition_days=3,
            page_size=page_size))

        # then
        self.assertEqual(len(tables), 3)
    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__equal_0(self):
        #given
        table2 = self.create_table_with(partition_id='20180214')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=0))

        #then
        self.assertEqual(len(tables), 0)\


    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__non_partitioned_table(self):
        #given
        non_partitioned_table = self.__create_single_table(project_id='p1', dataset_id='d1', table_id='t1')


        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=0))

        #then
        self.assertTrue(non_partitioned_table in tables)
        self.assertEqual(len(tables), 1)

    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__contains_todays_partition(self):
        #given
        table1 = self.create_table_with(partition_id='20180215')
        table2 = self.create_table_with(partition_id='20180214')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=1))

        #then
        self.assertTrue(table1 in tables)
        self.assertEqual(len(tables), 1)

    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__outside_of_scope(self):
        #given
        table2 = self.create_table_with(partition_id='20180214')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=1))

        #then
        self.assertTrue(table2 not in tables)
        self.assertEqual(len(tables), 0)
    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__more_than_one(self):
        #given
        table1 = self.create_table_with(partition_id='20180215')
        table2 = self.create_table_with(partition_id='20180214')
        table3 = self.create_table_with(partition_id='20180213')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=2))

        #then
        self.assertTrue(table1 in tables)
        self.assertTrue(table2 in tables)
        self.assertTrue(table3 not in tables)
        self.assertEqual(len(tables), 2)    \


    @freeze_time("2018-02-15")
    def test_get_tables_with_max_partition_days__empty_days(self):
        #given
        table1 = self.create_table_with(partition_id='20180215')
        table3 = self.create_table_with(partition_id='20180213')

        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', max_partition_days=2))

        #then
        self.assertTrue(table1 in tables)
        self.assertTrue(table3 not in tables)
        self.assertEqual(len(tables), 1)

    def test_should_filter_by_project(self):
        #given
        table_entities_creator.create_multiple_table_entities(1, 'p1', partition_id=None, dataset_id='d1')
        table_entities_creator.create_multiple_table_entities(1, 'p2', partition_id=None, dataset_id='d1')

        #when
        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd1', 20))

        #then
        self.assertEqual(len(tables), 1)

    def test_should_filter_by_dataset(self):
        #given
        table_entities_creator.create_multiple_table_entities(1, 'p1', partition_id=None, dataset_id='d1')
        table_entities_creator.create_multiple_table_entities(1, 'p1', partition_id=None, dataset_id='d2')

        #when
        #when
        tables = list(Table.get_tables_with_max_partition_days('p1', 'd2', 20))

        #then
        self.assertEqual(len(tables), 1)


    def test_get_table_should_return_correct_table_partition(self):
        # given
        table1 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl1',
                                                     datetime(2017, 12, 5),
                                                     'partition1')
        #
        table2 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl1',
                                                     datetime(2017, 12, 5),
                                                     'partition2')

        # when
        returned_table = Table \
            .get_table('proj1', 'dataset1', 'tbl1', 'partition1')

        # then
        self.assertEqual(returned_table, table1)

    def test_get_table_should_return_correct_non_partitioned_table(self):
        # given
        table1 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl1',
                                                     datetime(2017, 12, 5))

        table2 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl2',
                                                     datetime(2017, 12, 5))

        # when
        returned_table = Table.get_table('proj1', 'dataset1', 'tbl2')

        # then
        self.assertEqual(returned_table, table2)

    @freeze_time("2017-12-06")
    def test_get_tables_should_return_tables_from_given_project_and_dataset(
            self):
        # given
        table1 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl1',
                                                     datetime(2017, 12, 5))
        table2 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl2',
                                                     datetime(2017, 12, 4))

        table3 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset2',
                                                     'tbl3',
                                                     datetime(2017, 12, 4))

        table4 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl4',
                                                     datetime(2017, 12, 4),
                                                     '20171204')
        table5 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset2',
                                                     'tbl5',
                                                     datetime(2017, 12, 4),
                                                     '20171204')

        # when
        dataset1_tables = Table.get_tables('proj1', 'dataset1')

        # then
        self.assertEqual(list(dataset1_tables), [table4, table1, table2])

    @freeze_time("2017-12-06")
    def test_get_tables_should_return_tables_from_given_project(self):
        # given
        table1 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl1',
                                                     datetime(2017, 12, 5))
        table2 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj2', 'dataset1',
                                                     'tbl2',
                                                     datetime(2017, 12, 4))

        table3 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl3',
                                                     datetime(2017, 12, 4))

        table4 = table_entities_creator \
            .create_and_insert_table_with_one_backup('project2', 'dataset1',
                                                     'tbl4',
                                                     datetime(2017, 12, 4),
                                                     '20171204')
        table5 = table_entities_creator \
            .create_and_insert_table_with_one_backup('proj1', 'dataset1',
                                                     'tbl5',
                                                     datetime(2017, 12, 4),
                                                     '20171204')

        # when
        project1_tables = Table.get_tables('proj1', 'dataset1')

        # then
        self.assertEqual(list(project1_tables), [table5, table1, table3])
