import unittest
from datetime import datetime

from freezegun import freeze_time

from mock import patch
from src.backup.datastore.Backup import Backup
from src.retention.policy.filter.younger_than_7_months_filter import \
    YoungerThan7MonthsFilter
from src.commons.table_reference import TableReference
from tests.utils.backup_utils import create_backup


class TestYoungerThan7MonthsFilter(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default')\
            .start()
        self.under_test = YoungerThan7MonthsFilter()

    def tearDown(self):
        patch.stopall()

    @freeze_time("2017-08-20")
    def test_should_filter_out_only_backups_which_are_older_than_7_months_when_some_younger_backups_exists(self):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2017, 1, 18))
        b2 = create_backup(datetime(2017, 1, 19))
        b3 = create_backup(datetime(2017, 1, 20))
        b4 = create_backup(datetime(2017, 1, 21))
        backups = [b1, b2, b3, b4]
        expected_backups = [b3, b4]

        # when
        backups_to_retain = self.under_test.filter(
            backups=list(backups),
            table_reference=reference
        )
        # then
        self.sortAndAssertListEqual(expected_backups, backups_to_retain)

    @freeze_time("2017-08-20")
    def test_should_filter_out_nothing_if_all_backups_are_younger_than_7_months(self):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        b1 = create_backup(datetime(2017, 3, 19))
        b2 = create_backup(datetime(2017, 3, 20))
        b3 = create_backup(datetime(2017, 3, 21))
        backups = [b1, b2, b3]
        expected_backups = [b1, b2, b3]

        # when
        backups_to_retain = self.under_test.filter(
            backups=list(backups),
            table_reference=reference
        )
        # then
        self.sortAndAssertListEqual(expected_backups, backups_to_retain)

    @freeze_time("2017-08-20")
    def test_should_filter_out_all_but_single_backup_when_all_backups_are_older_than_7_months(self):  # nopep8 pylint: disable=C0301
        # given
        b1 = create_backup(datetime(2017, 1, 17))
        b2 = create_backup(datetime(2017, 1, 18))
        b3 = create_backup(datetime(2017, 1, 19))
        backups = [b1, b2, b3]
        expected_backups = [b3]
        # when
        backups_to_retain = self.under_test.filter(
            backups=backups,
            table_reference=None
        )
        # then
        self.sortAndAssertListEqual(expected_backups, backups_to_retain)

    def sortAndAssertListEqual(self, backup_list1, backup_list2):
        sorted_list1 = Backup.sort_backups_by_create_time_desc(backup_list1)
        sorted_list2 = Backup.sort_backups_by_create_time_desc(backup_list2)
        self.assertListEqual(sorted_list1, sorted_list2)
