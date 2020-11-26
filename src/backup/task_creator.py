import logging

from src.commons.tasks import Tasks


class TaskCreator(object):

    @staticmethod
    def schedule_tasks_for_partition_backup(project_id, dataset_id,
                                            table_id, partition_ids):
        logging.info(
            "Scheduling backup tasks for table: '%s:%s.%s partitions: %s'",
            project_id, dataset_id, table_id, partition_ids)

        tasks = []
        for partition_id in partition_ids:
            task = Tasks.create(
                method='GET',
                url='/tasks/backups/table/{}/{}/{}/{}'
                    .format(project_id, dataset_id, table_id, partition_id))
            tasks.append(task)
        Tasks.schedule('backup-worker', tasks)

    @staticmethod
    def schedule_tasks_for_tables_backup(project_id, dataset_id, table_ids):
        table_ids = list(table_ids)
        logging.info(
            "Scheduling backup tasks for dataset: '%s:%s tables: %s'",
            project_id, dataset_id, table_ids)

        tasks = []
        for table_id in table_ids:
            tasks.append(Tasks.create(
                method='GET',
                url='/tasks/backups/table/{0}/{1}/{2}'
                    .format(project_id, dataset_id, table_id)))
        Tasks.schedule('backup-worker', tasks)
