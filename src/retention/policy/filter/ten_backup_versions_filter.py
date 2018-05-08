from src.backup.datastore.Backup import Backup


class TenBackupVersionsFilter(object):
    NUMBER_OF_BACKUPS_TO_KEEP = 10

    def filter(self, backups, table_reference):
        sorted_backups = Backup.sort_backups_by_create_time_desc(backups)
        return sorted_backups[:self.NUMBER_OF_BACKUPS_TO_KEEP]
