import logging

from src.backup.abstract_backup_predicate import \
    AbstractBackupPredicate


class OnDemandBackupPredicate(AbstractBackupPredicate):

    def test(self, big_query_table_metadata, table_entity):
        if not self._is_possible_to_copy_table(big_query_table_metadata):
            return False

        logging.info("Performing on-demand backup for %s."
                     "It is performed without checking "
                     "if table aready has up to date backup",
                     big_query_table_metadata.table_reference())

        return True
