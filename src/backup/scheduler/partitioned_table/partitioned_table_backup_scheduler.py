import logging

from src.backup.scheduler.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.tasks import Tasks


class PartitionedTableBackupScheduler(object):

    def __init__(self):
        self.big_query = BigQuery()

    def schedule_backup(self, project_id, dataset_id, table_id):
        partitions = self.big_query.list_table_partitions(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id)
        partition_ids_to_backup = [partition['partitionId'] for partition in
                                   partitions]
        if not partition_ids_to_backup:
            logging.info(u"Table %s:%s.%s doesn't contain any partitions",
                         project_id, dataset_id, table_id)

        self._schedule_tasks_for_partition_backup(project_id, dataset_id,
                                                  table_id,
                                                  partition_ids_to_backup)

    @staticmethod
    def _schedule_tasks_for_partition_backup(project_id, dataset_id, table_id,
        partiton_ids):
        logging.info(
            u'Scheduling Partition Table Backup tasks for %s %s:%s.%s table partitions: %s.',
            len(partiton_ids), project_id, dataset_id, table_id, partiton_ids)

        tasks = []

        for partition_id in partiton_ids:
            tasks.append(
                TaskCreator.create_partition_table_backup_task(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    partition_id=partition_id)
            )
        Tasks.schedule('backup-worker', tasks)
