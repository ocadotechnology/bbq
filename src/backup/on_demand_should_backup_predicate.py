import logging

from src.backup.abstract_should_backup_predicate import \
    AbstractShouldBackupPredicate


class OnDemandShouldBackupPredicate(AbstractShouldBackupPredicate):

    def __init__(self, big_query_table_metadata):
        super(OnDemandShouldBackupPredicate, self).__init__(big_query_table_metadata)

    def _is_table_has_up_to_date_backup(self, table_entity):
        logging.info(
            "Performing on-demand backup for %s:%s.%s$%s. "
            "It is performed without checking if table aready has up to date backup",
            table_entity.project_id, table_entity.dataset_id,
            table_entity.table_id, table_entity.partition_id)
        return False
