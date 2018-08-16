import logging
from datetime import datetime

from src.commons.exceptions import NotFoundException
from src.backup.datastore.Table import Table


class BackupFinder(object):

    @classmethod
    def for_table(cls, table_reference, not_newer_than):
        if not not_newer_than:
            not_newer_than = datetime.utcnow()

        table_entity = cls.__get_table_entity(table_reference)
        backup = table_entity.last_backup_not_newer_than(not_newer_than)
        if backup is None:
            raise NotFoundException(
                'Backup not found for {} before {}'.format(
                    table_reference, not_newer_than))
        logging.info("backup: %s", backup)
        return backup

    @staticmethod
    def __get_table_entity(table_reference):
        table = Table.get_table(table_reference.project_id,
                                table_reference.dataset_id,
                                table_reference.table_id,
                                table_reference.partition_id)
        if table is None:
            raise NotFoundException(
                'Table not found in datastore: {}'.format(table_reference))
        logging.info("Datastore table: %s", table)
        return table
