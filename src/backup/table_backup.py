import logging

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.backup.backup_process import BackupProcess
from src.backup.table_partitions_backup_scheduler import \
    TablePartitionsBackupScheduler
from src.commons.big_query.big_query import BigQuery


class TableBackup(object):

    @staticmethod
    def start(table_reference):
        big_query = BigQuery()

        big_query_table_metadata = BigQueryTableMetadata.get_table_by_reference(table_reference)

        if big_query_table_metadata.is_daily_partitioned() and \
                not big_query_table_metadata.is_partition():
            logging.info('Table (%s/%s/%s) is partitioned',
                         table_reference.get_project_id(),
                         table_reference.get_dataset_id(),
                         table_reference.get_table_id())
            TablePartitionsBackupScheduler(table_reference,
                                           big_query).start()
        else:
            BackupProcess(table_reference=table_reference,
                          big_query=big_query,
                          big_query_table_metadata=big_query_table_metadata).start()
