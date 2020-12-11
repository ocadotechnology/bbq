import logging

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata


class SLIBackupTableNotSeenByCensusPredicate(object):

    def __init__(self, big_query, query_specification):
        self.big_query = big_query
        self.query_specification = query_specification

    def is_not_seen_by_census(self, sli_table):
        backup_table_reference = self.query_specification.to_backup_table_reference(sli_table)
        backup_table_metadata = BigQueryTableMetadata(
            self.big_query.get_table(
                project_id=backup_table_reference.project_id,
                dataset_id=backup_table_reference.dataset_id,
                table_id=backup_table_reference.table_id)
        )

        if not backup_table_metadata.table_exists():
            logging.info("Backup table doesn't exist: %s",
                         backup_table_reference)
            return False

        if not sli_table['backupLastModifiedTime']:
            if backup_table_metadata.table_size_in_bytes() == sli_table['backupEntityNumBytes']:
                logging.info(
                    "Backup table: %s exists although Census doesn't see it yet. "
                    "Backup table have the same number of bytes as saved in datastore.",
                    backup_table_reference)
                return True

        return False
