import unittest
from datetime import datetime

from freezegun import freeze_time

from mock import patch
from src.backup.datastore.Backup import Backup

from src.commons.table_reference import TableReference
from src.retention.policy.filter.only_one_version_old_backup_filter import \
    OnlyOneVersionForOldBackupFilter
from tests.utils.backup_utils import create_backup


class TestOnlyOneVersionForOldBackupFilter(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default')\
            .start()
        self.under_test = OnlyOneVersionForOldBackupFilter()

    def tearDown(self):
        patch.stopall()

    @freeze_time("2017-08-20")
    def test_should_retain_the_youngest_old_backup_and_young_backups(self):  # nopep8 pylint: disable=C0301
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        oldest_backup = create_backup(datetime(2017, 1, 18))
        youngest_old_backup = create_backup(datetime(2017, 1, 19))
        young_backup1 = create_backup(datetime(2017, 1, 20))
        young_backup2 = create_backup(datetime(2017, 1, 21))
        backups = [oldest_backup, youngest_old_backup, young_backup1, young_backup2]
        expected_backups = [youngest_old_backup, young_backup1, young_backup2]

        # when
        backups_to_retain = self.under_test.filter(
            backups=list(backups),
            table_reference=reference
        )
        # then
        self.sortAndAssertListEqual(expected_backups, backups_to_retain)

    @freeze_time("2017-08-20")
    def test_should_retain_all_if_all_backups_are_younger_than_7_months(self):  # nopep8 pylint: disable=C0301
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
        youngest_old_backup = create_backup(datetime(2017, 1, 19))
        backups = [b1, b2, youngest_old_backup]
        expected_backups = [youngest_old_backup]
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
