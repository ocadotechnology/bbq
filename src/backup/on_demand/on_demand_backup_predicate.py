import logging

from src.backup.abstract_backup_predicate import \
    AbstractBackupPredicate
from src.commons.exceptions import ParameterValidationException, NotFoundException


class OnDemandBackupPredicate(AbstractBackupPredicate):

    def test(self, big_query_table_metadata, table_entity):
        if big_query_table_metadata.is_daily_partitioned() and not big_query_table_metadata.is_partition():
            raise ParameterValidationException("Partition id is required for partitioned table in on-demand mode")

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
