import logging

from src.backup.backup_process import BackupProcess
from src.backup.table_partitions_backup_scheduler import \
    TablePartitionsBackupScheduler
from src.big_query.big_query import BigQuery


class TableBackup(object):

    @staticmethod
    def start(table_reference):
        big_query = BigQuery()
        big_query_table_metadata = big_query.get_table_or_partition(
            table_reference.get_project_id(),
            table_reference.get_dataset_id(),
            table_reference.get_table_id(),
            table_reference.get_partition_id()
        )

        if big_query_table_metadata.is_daily_partitioned() \
                and not big_query_table_metadata.is_empty():
            logging.info('Table (%s/%s/%s) is partitioned',
                         table_reference.get_project_id(),
                         table_reference.get_dataset_id(),
                         table_reference.get_table_id())
            TablePartitionsBackupScheduler(table_reference,
                                           big_query).start()
        else:
            BackupProcess(
                table_reference, big_query, big_query_table_metadata).start()
