import logging

from src.backup.datastore.Table import Table
from src.commons.collections import paginated
from src.commons.decorators.log_time import log_time
from src.commons.table_reference import TableReference
from src.restore.datastore.restore_item import RestoreItem
from src.restore.restoration_table_reference import RestoreTableReference


class DatasetRestoreItemsGenerator(object):

    @classmethod
    @log_time
    def generate_restore_items(cls, project_id, dataset_id, target_project_id,
                               target_dataset_id, max_partition_days):
        if max_partition_days:
            table_entities = Table \
                .get_tables_with_max_partition_days(project_id, dataset_id,
                                                    max_partition_days)
        else:
            table_entities = Table.get_tables(project_id, dataset_id)

        for table_entity_sublist in paginated(1000, table_entities):
            restore_items = []
            for table_entity, backup_entity in Table.get_last_backup_for_tables(
                    table_entity_sublist):
                if backup_entity is not None:
                    source_table_reference = \
                        RestoreTableReference.backup_table_reference(
                            table_entity, backup_entity)

                    target_table_reference = TableReference(
                        target_project_id,
                        target_dataset_id,
                        table_entity.table_id,
                        table_entity.partition_id
                    )

                    restore_item = RestoreItem.create(source_table_reference,
                                                      target_table_reference)
                    restore_items.append(restore_item)

            logging.info("Restore items generator yields %s restore items",
                         len(restore_items))
            yield restore_items
