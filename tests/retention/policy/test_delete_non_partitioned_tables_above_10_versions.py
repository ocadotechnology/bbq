import unittest
from random import shuffle

from nose_parameterized import parameterized
from src.backup.datastore.Backup import Backup
from src.retention.policy.delete_non_partitioned_tables_above_10_versions import \
    DeleteNonPartitionedTablesOlderThan10Versions
from src.commons.table_reference import TableReference
from tests.utils import backup_utils


class TestDeleteNonPartitionedTablesOlderThan10Versions(unittest.TestCase):
    def setUp(self):
        self.under_test = DeleteNonPartitionedTablesOlderThan10Versions()

    @parameterized.expand([[0], [1], [2], [5], [7], [10]])
    def test_should_not_delete_if_there_is_10_or_less_backups(self, count):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = backup_utils.create_backup_daily_sequence(count)

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=backups,
                                               table_reference=reference)
        # then
        self.assertFalse(eligible_for_deletion,
                         "Backup list should be empty, actual count: {}"
                         .format(len(eligible_for_deletion)))

    @parameterized.expand([[0], [1], [2], [5], [7], [10], [11], [15]])
    def test_should_not_delete_partitioned_tables(self, count):
        # given
        reference = TableReference('example-project-id',
                                       'example-dataset-id',
                                       'example-table-id',
                                       '20170601')
        backups = backup_utils.create_backup_daily_sequence(count)

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=backups,
                                               table_reference=reference)
        # then
        self.assertFalse(eligible_for_deletion,
                         "Backup list should be empty, actual count: {}"
                         .format(len(eligible_for_deletion)))

    def test_should_delete_backups_above_10_version(self):
        # given
        reference = TableReference('example-project-id', 'example-dataset-id',
                                   'example-table-id')
        backups = Backup.sort_backups_by_create_time_desc(
            backup_utils.create_backup_daily_sequence(14))
        expected_deleted_backups = backups[10:]
        shuffle(backups)

        # when
        eligible_for_deletion = self.under_test \
            .get_backups_eligible_for_deletion(backups=backups,
                                               table_reference=reference)
        # then
        self.assertListEqual(expected_deleted_backups, eligible_for_deletion)
