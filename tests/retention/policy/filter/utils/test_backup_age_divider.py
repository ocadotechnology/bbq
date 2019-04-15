from datetime import datetime
from unittest import TestCase

from freezegun import freeze_time

from src.retention.policy.filter.utils.backup_age_divider import \
    BackupAgeDivider
from tests.utils.backup_utils import create_backup


class TestBackupAgeDivider(TestCase):

    @freeze_time("2017-08-20")
    def test_should_divide_backups_for_young_and_old(self):
        # given
        oldest_backup = create_backup(datetime(2017, 1, 18))
        youngest_old_backup = create_backup(datetime(2017, 1, 19))
        young_backup1 = create_backup(datetime(2017, 1, 20))
        young_backup2 = create_backup(datetime(2017, 1, 21))
        backups = [oldest_backup, youngest_old_backup, young_backup1,
                   young_backup2]

        # when
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(
            backups)

        # then
        self.assertListEqual([young_backup1, young_backup2], young_backups)
        self.assertListEqual([oldest_backup, youngest_old_backup], old_backups)

    @freeze_time("2017-08-20")
    def test_should_divide_backups_preserving_order(self):
        # given
        oldest_backup = create_backup(datetime(2017, 1, 18))
        youngest_old_backup = create_backup(datetime(2017, 1, 19))
        young_backup1 = create_backup(datetime(2017, 1, 20))
        young_backup2 = create_backup(datetime(2017, 1, 21))
        backups = [youngest_old_backup, young_backup1, young_backup2,
                   oldest_backup]

        # when
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(
            backups)

        # then
        self.assertListEqual([young_backup1, young_backup2], young_backups)
        self.assertListEqual([youngest_old_backup, oldest_backup], old_backups)

    @freeze_time("2017-08-20")
    def test_should_divide_backups_if_any_young_backups(self):
        # given
        oldest_backup = create_backup(datetime(2017, 1, 18))
        youngest_old_backup = create_backup(datetime(2017, 1, 19))

        backups = [oldest_backup, youngest_old_backup]

        # when
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(
            backups)

        # then
        self.assertListEqual([], young_backups)
        self.assertListEqual([oldest_backup, youngest_old_backup], old_backups)

    @freeze_time("2017-08-20")
    def test_should_divide_backups_if_any_old_backups(self):
        # given
        young_backup1 = create_backup(datetime(2017, 1, 20))
        young_backup2 = create_backup(datetime(2017, 1, 21))
        backups = [young_backup1, young_backup2]

        # when
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(
            backups)

        # then
        self.assertListEqual([young_backup1, young_backup2], young_backups)
        self.assertListEqual([], old_backups)

    @freeze_time("2017-08-20")
    def test_should_divide_for_empty_lists_if_any_backups(self):
        # given
        backups = []

        # when
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(
            backups)

        # then
        self.assertListEqual([], young_backups)
        self.assertListEqual([], old_backups)
