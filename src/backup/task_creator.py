import logging

from src.commons.tasks import Tasks


class TaskCreator(object):

    @staticmethod
    def schedule_tasks_for_partition_backup(project_id, dataset_id,
                                            table_id, partition_ids):
        logging.info(
            "Scheduling backup tasks for table: '%s/%s/%s partitions: %s'",
            project_id, dataset_id, table_id, partition_ids)

        tasks = []
        for partition_id in partition_ids:
            task = Tasks.create(
                method='GET',
                url='/tasks/backups/table/{}/{}/{}/{}'
                    .format(project_id, dataset_id, table_id, partition_id))
            tasks.append(task)
        Tasks.schedule('backup-worker', tasks)
