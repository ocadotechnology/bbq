import logging

from src.backup.abstract_should_backup_predicate import \
    AbstractShouldBackupPredicate


class OnDemandShouldBackupPredicate(AbstractShouldBackupPredicate):

    def test(self, big_query_table_metadata, table_entity):
        if not self._is_possible_to_copy_table(big_query_table_metadata):
            return False

        if big_query_table_metadata.is_empty():
            logging.info('This table is empty')

        logging.info(
            "Performing on-demand backup for %s:%s.%s$%s. "
            "It is performed without checking if table aready has up to date backup",
            table_entity.project_id, table_entity.dataset_id,
            table_entity.table_id, table_entity.partition_id)

        return True
