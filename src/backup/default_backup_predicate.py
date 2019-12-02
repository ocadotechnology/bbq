import logging

from src.backup.abstract_backup_predicate import \
    AbstractBackupPredicate


class DefaultBackupPredicate(AbstractBackupPredicate):
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

    def test(self, big_query_table_metadata, table_entity):
        table_validation_status, table_validation_message = self._is_possible_to_copy_table(big_query_table_metadata)
        if not table_validation_status:
            return False

        last_backup = self.__get_last_table_backup_if_any(table_entity)

        if not last_backup:
            return True

        if big_query_table_metadata.is_empty() and last_backup.numBytes > 0:
            logging.info("Source table is empty. Not empty backup exists.")
            return False

        if self.__is_table_backup_up_to_date(big_query_table_metadata, last_backup):
            logging.info('Backup is up to date')
            return False

        return True

    def __get_last_table_backup_if_any(self, table_entity):
        if table_entity is None:
            return None
        last_backup = table_entity.last_backup
        if last_backup is None:
            logging.info('No backups so far')
            return None
        return last_backup

    def __is_table_backup_up_to_date(self, big_query_table_metadata, last_backup):
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

        source_table_num_bytes = big_query_table_metadata.table_size_in_bytes()
        if source_table_num_bytes != last_backup.numBytes:
            logging.info("Backup size is different than source. "
                         "Source: '%s', backup: '%s'",
                         source_table_num_bytes, last_backup.numBytes)
            return False

        return True

    def __format_timestamp(self, datetime):
        if datetime:
            return datetime.strftime(self.TIMESTAMP_FORMAT)
        else:
            return None
