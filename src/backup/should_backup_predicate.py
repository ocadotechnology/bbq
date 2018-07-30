import logging


class ShouldBackupPredicate(object):

    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, big_query_table_metadata):
        self.big_query_table_metadata = big_query_table_metadata

    def test(self, table_entity):
        if not self.big_query_table_metadata.table_exists():
            logging.info('Table not found (404)')
            return False
        if not self.big_query_table_metadata.is_schema_defined():
            logging.info('This table is without schema')
            return False
        if self.big_query_table_metadata.is_empty():
            logging.info('This table is empty')
        if self.big_query_table_metadata.is_external_or_view_type():
            return False
        if not self.__should_backup(table_entity):
            logging.info('Backup is up to date')
            return False
        return True

    # pylint: disable=R0201
    def __should_backup(self, table_entity):
        # TODO: change name of this class or split this method into two
        if table_entity is None:
            return True
        last_backup = table_entity.last_backup
        if last_backup is None:
            logging.info('No backups so far')
            return True
        source_table_last_modified_time = \
            self.big_query_table_metadata.get_last_modified_datetime()
        logging.info(
            "Last modification timestamps: last backup '%s', "
            "table metadata: '%s'",
            self.__format_timestamp(last_backup.last_modified),
            self.__format_timestamp(source_table_last_modified_time)
        )
        if source_table_last_modified_time > last_backup.last_modified:
            logging.info("Backup time is older than table metadata")
            return True
        return False

    def __format_timestamp(self, datetime):
        if datetime:
            return datetime.strftime(self.TIMESTAMP_FORMAT)
        else:
            return None
