import logging
from abc import abstractmethod


class AbstractShouldBackupPredicate(object):

    @abstractmethod
    def test(self, big_query_table_metadata, table_entity):
        raise NotImplementedError

    @staticmethod
    def _is_possible_to_copy_table(big_query_table_metadata):
        if not big_query_table_metadata.table_exists():
            logging.info('Table not found (404)')
            return False
        if not big_query_table_metadata.is_schema_defined():
            logging.info('This table is without schema')
            return False
        if big_query_table_metadata.is_external_or_view_type():
            return False
        return True
