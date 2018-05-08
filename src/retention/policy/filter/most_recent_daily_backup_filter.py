import logging

from src.backup.datastore.Backup import Backup


class MostRecentDailyBackupFilter(object):

    def filter(self, backups, table_reference):
        sorted_backups = Backup.sort_backups_by_create_time_desc(backups)
        deduplicated_backups = self.\
            __get_most_recent_backup_per_day(sorted_backups)
        if len(sorted_backups) != len(deduplicated_backups):
            logging.debug("Deduplicated backups from %s to %s for table: '%s'",
                          len(sorted_backups), len(deduplicated_backups),
                          table_reference)
        return deduplicated_backups

    @staticmethod
    def __get_most_recent_backup_per_day(sorted_backups):
        checked_dates = []
        deduplicated_backups = []
        for b in sorted_backups:
            date = b.created.date()
            if date not in checked_dates:
                checked_dates.append(date)
                deduplicated_backups.append(b)
        return deduplicated_backups
