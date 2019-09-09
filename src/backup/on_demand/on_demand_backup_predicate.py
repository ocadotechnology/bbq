import logging

from src.backup.abstract_backup_predicate import \
    AbstractBackupPredicate
from src.commons.exceptions import ParameterValidationException, NotFoundException


class OnDemandBackupPredicate(AbstractBackupPredicate):

    def test(self, big_query_table_metadata, table_entity):
        table_validation_status, table_validation_message = self._is_possible_to_copy_table(big_query_table_metadata)
        if not table_validation_status:
            if table_validation_message == "Table not found":
                raise NotFoundException(table_validation_message)
            else:
                raise ParameterValidationException(table_validation_message)

        logging.info("Performing on-demand backup for %s."
                     "It is performed without checking "
                     "if table already has up to date backup",
                     big_query_table_metadata.table_reference())

        return True
