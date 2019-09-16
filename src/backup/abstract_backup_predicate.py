import logging
from abc import abstractmethod


class AbstractBackupPredicate(object):

    @abstractmethod
    def test(self, big_query_table_metadata, table_entity):
        raise NotImplementedError

    @staticmethod
    def _is_possible_to_copy_table(big_query_table_metadata):
        if not big_query_table_metadata.table_exists():
            logging.info('Table not found (404)')
            return False, "Table not found"
        if not big_query_table_metadata.is_schema_defined():
            logging.info('This table is without schema')
            return False, "This table is without schema"
        if big_query_table_metadata.is_external_or_view_type():
            logging.info('This table is external or view type')
            return False, "This table is external or view type"
        if big_query_table_metadata.is_empty():
            logging.info('This table is empty')
            return True, "This table is empty"

        return True, "This table is valid"
