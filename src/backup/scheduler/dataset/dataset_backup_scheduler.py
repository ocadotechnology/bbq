import logging

from src.backup.scheduler.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.tasks import Tasks


class DatasetBackupScheduler(object):

    def __init__(self):
        self.big_query = BigQuery()

    def schedule_backup(self, project_id, dataset_id, page_token=None):
        table_ids_to_backup, next_page_token = self.big_query.list_table_ids(
            project_id=project_id,
            dataset_id=dataset_id,
            page_token=page_token)

        self._schedule_tasks_for_tables_backup(project_id, dataset_id,
                                               table_ids_to_backup)

        if next_page_token:
            logging.info(
                u'Scheduling Dataset Backup Scheduler task for %s:%s, page_token: %s',
                project_id, dataset_id, page_token)

            Tasks.schedule('backup-scheduler',
                           TaskCreator.create_dataset_backup_scheduler_task(
                               project_id=project_id,
                               dataset_id=dataset_id,
                               page_token=next_page_token)
                           )

    @staticmethod
    def _schedule_tasks_for_tables_backup(project_id, dataset_id, table_ids):
        logging.info(
            u'Scheduling Table Backup tasks for %s %s:%s dataset tables: %s',
            len(table_ids), project_id, dataset_id, table_ids)

        tasks = []

        for table_id in table_ids:
            tasks.append(
                TaskCreator.create_table_backup_task(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_id)
            )
        Tasks.schedule('backup-worker', tasks)
