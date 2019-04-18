import unittest
from datetime import datetime

from freezegun import freeze_time
from mock import patch

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.table_reference import TableReference
from src.retention.policy.retention_policy import RetentionPolicy
from tests.utils.backup_utils import create_backup_daily_sequence, create_backup


class TestRetentionPolicy(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self.under_test = RetentionPolicy()

    def tearDown(self):
        patch.stopall()

    @freeze_time("2017-08-20")
    def test_should_not_delete_if_single_young_backup(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        single_young_backup = create_backup(datetime(2017, 8, 1))
        all_backups = list([single_young_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @freeze_time("2017-08-20")
    def test_should_not_delete_if_less_than_10_young_backups(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(3,
                                                     start_date=datetime(2017, 8, 1))
        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(young_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @freeze_time("2017-08-20")
    def test_should_not_delete_if_10_young_backups(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(10,
                                                     start_date=datetime(2017, 8, 1))
        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(young_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @freeze_time("2017-08-20")
    def test_should_delete_oldest_if_11_young_backups(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(11,
                                                     start_date=datetime(2017, 8, 1))
        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(young_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([young_backups[10]], eligible_for_deletion)

    @freeze_time("2017-08-20")
    def test_should_delete_same_day_duplicates_backups(self):
        #given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')

        first_5_backups = create_backup_daily_sequence(
            5,
            start_date=datetime(2017, 6, 1, 12)
        )
        second_5_backups = create_backup_daily_sequence(
            5,
            start_date=datetime(2017, 6, 6, 12)
        )
        first_5_backups_duplicated = create_backup_daily_sequence(
            5,
            start_date=datetime(2017, 6, 1, 14)
        )

        backups = list(
            first_5_backups + second_5_backups + first_5_backups_duplicated
        )
        backups_expected_for_deletion = list(first_5_backups)

        #when
        eligible_for_deletion = \
            self.under_test.get_backups_eligible_for_deletion(
                backups=list(backups),
                table_reference=reference)
        #then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )
    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_many_today_duplicates_and_11th_young_version_after_deduplication_and_retain_old_backup(
            self, _1, _2, _3):
        #given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')

        young_backups = create_backup_daily_sequence(10,
                                                     start_date=datetime(2017,
                                                                         8, 1))
        newest_duplicated_backup = create_backup(datetime(2017, 8, 19, 10))

        today_duplicated_backups = [newest_duplicated_backup,
                                    create_backup(datetime(2017, 8, 19, 9)),
                                    create_backup(datetime(2017, 8, 19, 8)),
                                    create_backup(datetime(2017, 8, 19, 7))]

        old_backup = create_backup(datetime(2016, 8, 19, 10))

        backups = list(
            young_backups + today_duplicated_backups + [old_backup]
        )
        backups_expected_for_deletion = list([young_backups[9]] + today_duplicated_backups[1:])

        #when
        eligible_for_deletion = \
            self.under_test.get_backups_eligible_for_deletion(
                backups=list(backups),
                table_reference=reference)

        #then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_not_delete_if_single_old_backup(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')

        single_old_backup = create_backup(datetime(2016, 8, 1))

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list([single_old_backup]),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_older_backup_if_two_old_backups(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        older_old_backup = create_backup(datetime(2016, 7, 31))
        old_backup = create_backup(datetime(2016, 8, 1))
        all_backups = list([older_old_backup, old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=all_backups,
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([older_old_backup], eligible_for_deletion)


    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_not_delete_if_single_young_backup_and_single_old_backup(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backup = create_backup(datetime(2017, 8, 1))
        old_backup = create_backup(datetime(2016, 8, 1))
        all_backups = [young_backup, old_backup]

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)


    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_not_delete_if_less_than_10_young_backup_and_single_old_backup(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(3,
                                                     start_date=datetime(2017, 8, 1))
        old_backup = create_backup(datetime(2016, 8, 1))
        all_backups = list(young_backups + [old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_not_delete_if_10_young_backup_and_single_old_backup(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(10,
                                                     start_date=datetime(2017, 8, 1))
        old_backup = create_backup(datetime(2016, 8, 1))
        all_backups = list(young_backups + [old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_oldest_young_backup_if_11_young_backup_and_single_old_backup(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = create_backup_daily_sequence(11,
                                                     start_date=datetime(2017, 8, 1))
        old_backup = create_backup(datetime(2016, 8, 1))
        all_backups = list(young_backups + [old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([young_backups[10]], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_older_old_backup_if_two_old_and_single_young_backups(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        older_old_backup = create_backup(datetime(2016, 7, 31))
        old_backup = create_backup(datetime(2016, 8, 1))
        young_backup = create_backup(datetime(2017, 8, 1))
        all_backups = list([older_old_backup, old_backup, young_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=all_backups,
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([older_old_backup], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_older_old_backup_if_two_old_and_less_than_10_young_backups(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        older_old_backup = create_backup(datetime(2016, 7, 31))
        old_backup = create_backup(datetime(2016, 8, 1))
        young_backups = create_backup_daily_sequence(3,
                                                     start_date=datetime(2017, 8, 1))
        all_backups = list(young_backups + [older_old_backup, old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=all_backups,
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([older_old_backup], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_older_old_backup_if_two_old_and_10_young_backups(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        older_old_backup = create_backup(datetime(2016, 7, 31))
        old_backup = create_backup(datetime(2016, 8, 1))
        young_backups = create_backup_daily_sequence(10,
                                                     start_date=datetime(2017, 8, 1))
        all_backups = list(young_backups + [older_old_backup, old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=all_backups,
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([older_old_backup], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_delete_older_old_and_oldest_young_backup_if_two_old_and_11_young_backups(self, _1,_2,_3):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        older_old_backup = create_backup(datetime(2016, 7, 31))
        old_backup = create_backup(datetime(2016, 8, 1))
        young_backups = create_backup_daily_sequence(11,
                                                     start_date=datetime(2017, 8, 1))
        all_backups = list(young_backups + [older_old_backup,
                                            old_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=all_backups,
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([older_old_backup, young_backups[10]], eligible_for_deletion)

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 19)))
    @freeze_time("2017-08-20")
    def test_should_leave_youngest_backup_from_the_same_day_when_source_data_exists(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2017, 1, 1, hour=13, minute=15))
        b2 = create_backup(datetime(2017, 1, 1, hour=16, minute=30))
        backups = [b1, b2]
        backups_expected_for_deletion = [b1]

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 1, 19)))
    @freeze_time("2017-08-20")
    def test_remove_all_backups_if_source_table_doesnt_exists_for_min_7_months(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2017, 1, 17))
        b2 = create_backup(datetime(2017, 1, 18))
        backups = [b1, b2]
        backups_expected_for_deletion = [b1, b2]

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference', return_value=BigQueryTableMetadata(None))
    @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 8, 1)))
    @freeze_time("2017-08-20")
    def test_should_not_remove_any_backups_if_source_table_was_deleted_less_than_seven_months_ago(self, _, _1, _2):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backup = create_backup(datetime(2017, 8, 1))
        old_backup = create_backup(datetime(2016, 1, 17))

        backups = [young_backup, old_backup]
        backups_expected_for_deletion = []

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )

    @freeze_time("2017-08-20")
    def test_should_remove_above_last_10_young_backups(self):
        #given
        reference = TableReference(
            'example-project-id',
            'example-dataset-id',
            'example-table-id'
        )
        oldest_backup = create_backup(datetime(2017, 6, 1))
        _10_last_backups = create_backup_daily_sequence(
            10,
            start_date=datetime(2017, 6, 2)
        )

        backups = list(_10_last_backups)
        backups.append(oldest_backup)

        backups_expected_for_deletion = [oldest_backup]

        #when
        eligible_for_deletion = \
            self.under_test.get_backups_eligible_for_deletion(
                backups=list(backups),
                table_reference=reference)
        #then
        self.sortAndAssertListEqual(
            backups_expected_for_deletion,
            eligible_for_deletion
        )

    def sortAndAssertListEqual(self, backup_list1, backup_list2):
        sorted_list1 = Backup.sort_backups_by_create_time_desc(backup_list1)
        sorted_list2 = Backup.sort_backups_by_create_time_desc(backup_list2)
        self.assertListEqual(sorted_list1, sorted_list2)
