import unittest
from datetime import datetime
from random import shuffle

from freezegun import freeze_time
from nose_parameterized import parameterized

from src.commons.table_reference import TableReference
from src.retention.policy.filter.ten_young_backup_versions_filter import \
    TenYoungBackupVersionsFilter
from tests.utils import backup_utils


class TestTenYoungBackupVersionsFilter(unittest.TestCase):
    def setUp(self):
        self.under_test = TenYoungBackupVersionsFilter()

    @parameterized.expand([[0], [1], [2], [5], [7], [10]])
    @freeze_time("2019-08-01")
    def test_should_not_filter_out_if_there_is_10_or_less_young_table_backups(
            self,
            count
    ):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = backup_utils.create_backup_daily_sequence(count,
                                                            start_date=datetime(2019, 7, 1))

        # when
        backups_to_retain = self.under_test.filter(list(backups), reference)
        # then
        self.assertListEqual(backups_to_retain, backups)

    @parameterized.expand(
        [[TableReference('example-project-id', 'example-dataset-id',
                         'example-table-id')],
         [TableReference('example-project-id',
                             'example-dataset-id',
                             'example-table-id',
                             '20170601')]])
    @freeze_time("2019-08-01")
    def test_should_filter_out_young_backups_above_10_version(self, reference):
        # given
        backups = backup_utils.create_backup_daily_sequence(14, start_date=datetime(2019, 7, 1))
        expected_retained_backups = backups[:10]
        shuffle(backups)

        # when
        backups_to_retain = self.under_test.filter(backups, reference)
        # then
        self.assertListEqual(backups_to_retain, expected_retained_backups)

    @freeze_time("2019-09-02")
    def test_should_filter_out_young_backups_above_10_version_but_retain_old_backups(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        young_backups = backup_utils.create_backup_daily_sequence(14, start_date=datetime(2019, 8, 15))
        old_backups = backup_utils.create_backup_daily_sequence(3, start_date=datetime(2019, 1, 1))
        all_backups = list(young_backups + old_backups)

        expected_retained_backups = list(young_backups[:10] + old_backups)
        shuffle(all_backups)

        # when
        backups_to_retain = self.under_test.filter(all_backups, reference)
        # then
        self.assertListEqual(backups_to_retain, expected_retained_backups)