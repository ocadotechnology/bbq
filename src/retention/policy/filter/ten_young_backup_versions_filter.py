from src.backup.datastore.Backup import Backup
from src.retention.policy.filter.utils.backup_age_divider import \
    BackupAgeDivider


class TenYoungBackupVersionsFilter(object):
    NUMBER_OF_BACKUPS_TO_KEEP = 10

    def filter(self, backups, table_reference):
        sorted_backups = Backup.sort_backups_by_create_time_desc(backups)
        young_backups, old_backups = BackupAgeDivider.divide_backups_by_age(sorted_backups)
        return young_backups[:self.NUMBER_OF_BACKUPS_TO_KEEP] + old_backups
