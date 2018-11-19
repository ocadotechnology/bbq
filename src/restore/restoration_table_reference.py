from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class RestoreTableReference(object):

    @staticmethod
    def backup_table_reference(table_entity, backup_entity):
        return TableReference(
            configuration.backup_project_id,
            backup_entity.dataset_id,
            backup_entity.table_id,
            table_entity.partition_id)

    @staticmethod
    def target_table_reference(table_entity, target_project_id, target_dataset_id):
        return TableReference(
            target_project_id,
            target_dataset_id,
            table_entity.table_id,
            table_entity.partition_id
        )
