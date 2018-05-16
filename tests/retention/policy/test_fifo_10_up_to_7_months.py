import unittest
from datetime import datetime

from freezegun import freeze_time

from mock import patch
from src.backup.datastore.Backup import Backup
from src.retention.policy.fifo_10_up_to_7_months import Fifo10UpTo7Months
from src.table_reference import TableReference
from tests.utils.backup_utils import create_backup_daily_sequence, create_backup


class TestFifo10UpTo7Months(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self.under_test = Fifo10UpTo7Months()

    def tearDown(self):
        patch.stopall()

    @freeze_time("2017-08-20")
    def test_should_delete_only_backups_which_are_older_than_7_months_and_are_above_10th_version(self):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = create_backup_daily_sequence(20,
                                               start_date=datetime(2016, 12, 1))
        youngest_backup = create_backup(datetime(2017, 8, 1))
        all_backups = list(backups + [youngest_backup])

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(all_backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual(backups, eligible_for_deletion)

    @freeze_time("2017-08-20")
    def test_should_delete_single_backup_which_is_older_than_7_months_and_is_11th_version(self):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = create_backup_daily_sequence(10,
                                               start_date=datetime(2017, 6, 1))
        oldest_backup = create_backup(datetime(2016, 12, 31))
        backups.append(oldest_backup)

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=list(backups),
                                               table_reference=reference)
        # then
        self.sortAndAssertListEqual([oldest_backup], eligible_for_deletion)


    @patch('src.big_query.big_query.BigQuery.get_table_by_reference.return_value.table_exists.return_value', True) # nopep8 pylint: disable=C0301
    @patch('src.big_query.big_query.BigQuery.get_table_by_reference')
    @freeze_time("2017-08-20")
    def test_should_leave_youngest_backup_from_the_same_day_when_source_data_exists(self, _):  # nopep8 pylint: disable=C0301
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

    # @patch('src.big_query.big_query.BigQuery.get_table_by_reference.return_value.table_exists.return_value', False) # nopep8 pylint: disable=C0301
    # @patch('src.big_query.big_query.BigQuery.get_table_by_reference')
    # @patch.object(Backup, 'get_table', return_value=Table(last_checked=datetime(2017, 1, 19)))
    # @freeze_time("2017-08-20")
    # def test_remove_all_backups_if_source_table_doesnt_exists_for_min_7_months(self, _, _1):  # nopep8 pylint: disable=C0301
    #     # given
    #     reference = TableReference('example-project-id', 'example-dataset-id',
    #                                'example-table-id')
    #     b1 = create_backup(datetime(2017, 1, 17))
    #     b2 = create_backup(datetime(2017, 1, 18))
    #     backups = [b1, b2]
    #     backups_expected_for_deletion = [b1, b2]
    #
    #     # when
    #     eligible_for_deletion = self.under_test \
    #         .get_backups_eligible_for_deletion(backups=list(backups),
    #                                            table_reference=reference)
    #     # then
    #     self.sortAndAssertListEqual(
    #         backups_expected_for_deletion,
    #         eligible_for_deletion
    #     )

    @freeze_time("2017-08-20")
    def test_should_remove_above_last_10_backups(self):
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


    @freeze_time("2017-08-20")
    def test_should_first_remove_same_day_duplicates_backups(self):
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

    def sortAndAssertListEqual(self, backup_list1, backup_list2):
        sorted_list1 = Backup.sort_backups_by_create_time_desc(backup_list1)
        sorted_list2 = Backup.sort_backups_by_create_time_desc(backup_list2)
        self.assertListEqual(sorted_list1, sorted_list2)
