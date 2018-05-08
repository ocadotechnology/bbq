import logging

from src.backup.datastore.Backup import Backup


class DeleteTablesOlderThan10Versions(object):
    NUMBER_OF_BACKUPS_TO_KEEP = 10

    @classmethod
    def get_backups_eligible_for_deletion(cls, backups, table_reference):
        backups = Backup.sort_backups_by_create_time_desc(backups)

        backups_eligible_for_deletion = backups[cls.NUMBER_OF_BACKUPS_TO_KEEP:]
        logging.info("%s out of %s backups selected for deletion for "
                     "table: '%s'", len(backups_eligible_for_deletion),
                     len(backups), table_reference)
        return backups_eligible_for_deletion
