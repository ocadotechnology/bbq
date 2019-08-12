import datetime
import logging


class SLITableHasAnyBackupPredicate(object):

    def has_any_backup(self, sli_table_entry):
        beginning_of_time = datetime.datetime.utcfromtimestamp(0)
        backup_created = datetime.datetime.utcfromtimestamp(
            sli_table_entry["backupCreated"])

        if backup_created > beginning_of_time:
            logging.info("Table has some backup.")
            return True
        else:
            logging.info("Table has no backup.")
            return False
