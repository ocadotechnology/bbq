import logging

from src.backup.backup_process import BackupProcess
from src.backup.scheduler.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.tasks import Tasks


class TableBackup(object):

    @staticmethod
    def start(table_reference):
        big_query = BigQuery()

        big_query_table_metadata = BigQueryTableMetadata.get_table_by_reference(
            table_reference)

        if big_query_table_metadata.is_daily_partitioned() and \
            not big_query_table_metadata.is_partition():
            logging.info(u'Table %s:%s.%s is partitioned',
                         table_reference.get_project_id(),
                         table_reference.get_dataset_id(),
                         table_reference.get_table_id())
            TableBackup._schedule_partitioned_table_backup_scheduler_task(
                table_reference)

        else:
            BackupProcess(table_reference=table_reference,
                          big_query=big_query,
                          big_query_table_metadata=big_query_table_metadata).start()

    @staticmethod
    def _schedule_partitioned_table_backup_scheduler_task(table_reference):
        Tasks.schedule('backup-scheduler',
                       TaskCreator.create_partitioned_table_backup_scheduler_task(
                           project_id=table_reference.get_project_id(),
                           dataset_id=table_reference.get_dataset_id(),
                           table_id=table_reference.get_table_id()))
