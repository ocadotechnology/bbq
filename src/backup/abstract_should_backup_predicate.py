import logging
from abc import abstractmethod


class AbstractShouldBackupPredicate(object):
    def __init__(self, big_query_table_metadata):
        self.big_query_table_metadata = big_query_table_metadata

    def test(self, table_entity):

        if not self.__is_possible_to_copy_table():
            return False

        if self.big_query_table_metadata.is_empty():
            logging.info('This table is empty')

        if self._is_table_has_up_to_date_backup(table_entity):
            logging.info('Backup is up to date')
            return False

        return True

    def __is_possible_to_copy_table(self):
        if not self.big_query_table_metadata.table_exists():
            logging.info('Table not found (404)')
            return False
        if not self.big_query_table_metadata.is_schema_defined():
            logging.info('This table is without schema')
            return False
        if self.big_query_table_metadata.is_external_or_view_type():
            return False
        return True

    @abstractmethod
    def _is_table_has_up_to_date_backup(self, table_entity):
        pass
