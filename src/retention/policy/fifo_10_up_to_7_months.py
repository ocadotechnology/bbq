import logging

from src.retention.policy.filter.grace_period_after_deletion_filter import \
    GracePeriodAfterDeletionFilter
from src.retention.policy.filter.most_recent_daily_backup_filter import \
    MostRecentDailyBackupFilter
from src.retention.policy.filter.ten_backup_versions_filter import \
    TenBackupVersionsFilter
from src.retention.policy.filter.younger_than_7_months_filter import \
    YoungerThan7MonthsFilter


class Fifo10UpTo7Months(object):
    def __init__(self):
        self.filters = [MostRecentDailyBackupFilter(),
                        TenBackupVersionsFilter(),
                        YoungerThan7MonthsFilter(),
                        GracePeriodAfterDeletionFilter()]

    def get_backups_eligible_for_deletion(self, backups, table_reference):
        backups_to_retain = backups
        for policy_filter in self.filters:
            filtered_backups = policy_filter.filter(backups=backups_to_retain,
                                                    table_reference=table_reference)

            logging.info("%s backups were filtered by using filter %s ",
                         len(backups_to_retain) - len(filtered_backups),
                         type(policy_filter).__name__)
            backups_to_retain = filtered_backups

        backups_to_delete = [b for b in backups
                             if b not in backups_to_retain]
        logging.info("%s out of %s backups "
                     "selected for deletion for table '%s'.",
                     len(backups_to_delete), len(backups), table_reference)

        return backups_to_delete
