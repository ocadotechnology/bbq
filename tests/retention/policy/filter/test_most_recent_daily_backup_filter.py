import unittest
from datetime import datetime

from src.retention.policy.filter.most_recent_daily_backup_filter import \
    MostRecentDailyBackupFilter
from src.commons.table_reference import TableReference
from tests.utils.backup_utils import create_backup


class TestMostRecentDailyBackupFilter(unittest.TestCase):

    def setUp(self):
        self.under_test = MostRecentDailyBackupFilter()
        self.reference = TableReference('example-project-id',
                                        'example-dataset-id',
                                        'example-table-id')

    def test_return_the_same_backups(self):
        # given
        backups = [create_backup(datetime(2017, 2, 1))]

        # when
        actual_backups = self.under_test.filter(list(backups), self.reference)

        # then
        self.assertListEqual(backups, actual_backups)

    def test_return_the_most_recent_backup_from_the_same_day(self):
        # given
        b1 = create_backup(datetime(2017, 2, 1, hour=15))
        b2 = create_backup(datetime(2017, 2, 1, hour=16))
        b3 = create_backup(datetime(2017, 2, 1, hour=17))

        backups = [b1, b3, b2]
        expected_backups = [b3]

        # when
        actual_backups = self.under_test.filter(list(backups), self.reference)

        # then
        self.assertListEqual(expected_backups, actual_backups)

    def test_deduplicate_backups_from_the_2_days(self):
        # given
        b1 = create_backup(datetime(2017, 2, 1))
        b2 = create_backup(datetime(2017, 2, 2))

        backups = [b1, b1, b2, b2]
        expected_backups = [b2, b1]

        # when
        actual_backups = self.under_test.filter(list(backups), self.reference)

        # then
        self.assertListEqual(expected_backups, actual_backups)

    def test_deduplicate_backups_from_the_5_days(self):
        # given
        b1 = create_backup(datetime(2017, 2, 1))
        b2 = create_backup(datetime(2017, 2, 2))
        b3 = create_backup(datetime(2017, 2, 3))
        b4 = create_backup(datetime(2017, 2, 4))
        b5 = create_backup(datetime(2017, 2, 5))

        backups = [b1, b2, b2, b3, b4, b4, b5]
        expected_backups = [b5, b4, b3, b2, b1]

        # when
        actual_backups = self.under_test.filter(list(backups), self.reference)

        # then
        self.assertListEqual(expected_backups, actual_backups)
