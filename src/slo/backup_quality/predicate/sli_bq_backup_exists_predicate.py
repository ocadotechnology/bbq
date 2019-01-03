import logging


class SLIBQBackupExistsPredicate(object):

    def exists(self, sli_table_entry):
        if sli_table_entry["backupNumBytes"]:
            logging.info("Table has backup table in BQ")
            return True
        else:
            logging.info("Table doesn't have backup table in BQ")
            return False
