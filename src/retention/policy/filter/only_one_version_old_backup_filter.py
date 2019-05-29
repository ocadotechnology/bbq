
from src.backup.datastore.Backup import Backup
from src.retention.policy.filter.utils.backup_age_divider import \
    BackupAgeDivider


class OnlyOneVersionForOldBackupFilter(object):

    def filter(self, backups, table_reference):
        sorted_backups = Backup.sort_backups_by_create_time_desc(backups)

        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(sorted_backups)

        backups_to_retain = young_backups
        if old_backups:
            backups_to_retain.append(old_backups[0])

        return backups_to_retain
