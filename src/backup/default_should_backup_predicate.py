import logging

from src.backup.abstract_should_backup_predicate import \
    AbstractShouldBackupPredicate


class DefaultShouldBackupPredicate(AbstractShouldBackupPredicate):
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

    def test(self, big_query_table_metadata, table_entity):
        if not self._is_possible_to_copy_table(big_query_table_metadata):
            return False

        if big_query_table_metadata.is_empty():
            logging.info('This table is empty')

        if self._is_table_has_up_to_date_backup(big_query_table_metadata, table_entity):
            logging.info('Backup is up to date')
            return False

        return True

    def _is_table_has_up_to_date_backup(self, big_query_table_metadata, table_entity):
        # TODO: change name of this class or split this method into two
        if table_entity is None:
            return False
        last_backup = table_entity.last_backup
        if last_backup is None:
            logging.info('No backups so far')
            return False
        source_table_last_modified_time = \
            big_query_table_metadata.get_last_modified_datetime()
        logging.info(
            "Last modification timestamps: last backup '%s', "
            "table metadata: '%s'",
            self.__format_timestamp(last_backup.last_modified),
            self.__format_timestamp(source_table_last_modified_time)
        )
        if source_table_last_modified_time > last_backup.last_modified:
            logging.info("Backup time is older than table metadata")
            return False
        return True

    def __format_timestamp(self, datetime):
        if datetime:
            return datetime.strftime(self.TIMESTAMP_FORMAT)
        else:
            return None
