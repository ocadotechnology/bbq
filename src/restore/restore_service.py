import logging
from datetime import datetime

from commons.camel_case_converter import dict_to_camel_case
from src.backup.datastore.Table import Table
from src.restore.big_query_table_restorer import BigQueryTableRestorer
from src.restore.restoration_table_reference import RestoreTableReference
from src.table_reference import TableReference


class TableNotFoundException(Exception):
    pass


class BackupNotFoundException(Exception):
    pass


class Restoration(object):
    def __init__(self, source_project_id, source_dataset_id, source_table_id,
                 target_dataset_id,
                 restoration_date, source_partition_id):
        self.source_project_id = source_project_id
        self.source_dataset_id = source_dataset_id
        self.source_table_id = source_table_id
        self.target_dataset_id = target_dataset_id
        self.restoration_date = restoration_date
        self.source_partition_id = source_partition_id

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return str(self.__dict__)


class RestoreService(object):
    def try_to_restore(self, restoration):
        table_entity = self.__get_table_entity(restoration)
        backup_entity = self.__get_backup_entity_to_restore(table_entity,
                                                            restoration)

        backup_table_reference = RestoreTableReference.backup_table_reference(
            table_entity, backup_entity)

        restore_table_reference = RestoreTableReference.target_table_reference(
            table_entity, restoration.target_dataset_id)

        restored_table_metadata = BigQueryTableRestorer().restore_table(
            backup_table_reference, restore_table_reference
        )
        logging.info("Table %s has been restored to %s",
                     backup_table_reference, restore_table_reference)
        return {
            'status': 'success',
            'size_in_bytes': restored_table_metadata.table_size_in_bytes(),
            'rows_count': restored_table_metadata.table_rows_count(),
            'backup_table_reference': dict_to_camel_case(
                backup_table_reference.__dict__),
            'restore_table_reference': dict_to_camel_case(
                restore_table_reference.__dict__),
            'source_table_last_modified_time':
                backup_entity.last_modified.isoformat(),
            'backup_creation_time':
                backup_entity.created.isoformat()
        }

    def __get_backup_entity_to_restore(self, table_entity, restoration):
        restoration_point = datetime.strptime(
            '{} 23:59:59'.format(restoration.restoration_date),
            '%Y-%m-%d %H:%M:%S'
        )
        backup_to_restore = table_entity.last_backup_not_newer_than(
            restoration_point)
        if backup_to_restore is None:
            raise BackupNotFoundException(
                'Backup not found for {} before {}'.format(
                    TableReference(restoration.source_project_id,
                                   restoration.source_dataset_id,
                                   restoration.source_table_id),
                    restoration_point)
            )
        logging.info("backup: %s", backup_to_restore)
        return backup_to_restore

    def __get_table_entity(self, restoration):
        table = Table.get_table(
            restoration.source_project_id,
            restoration.source_dataset_id,
            restoration.source_table_id,
            restoration.source_partition_id
        )
        if table is None:
            raise TableNotFoundException(
                'Table not found in datastore: {}'.format(
                    TableReference(restoration.source_project_id,
                                   restoration.source_dataset_id,
                                   restoration.source_table_id,
                                   restoration.source_partition_id)))
        logging.info("Datastore table: %s", table)
        return table
