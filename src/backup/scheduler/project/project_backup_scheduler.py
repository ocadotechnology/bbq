import logging

from src.backup.scheduler.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.tasks import Tasks


class ProjectBackupScheduler(object):

    def __init__(self):
        self.big_query = BigQuery()

    def schedule_backup(self, project_id, page_token=None):
        dataset_ids_to_backup, next_page_token = self.big_query.list_dataset_ids(
            project_id=project_id,
            page_token=page_token)

        self._schedule_dataset_backup_scheduler_tasks(project_id,
                                                      dataset_ids_to_backup)

        if next_page_token:
            logging.info(
                u'Scheduling Project Backup Scheduler task for %s, page_token: %s',
                project_id, next_page_token)
            Tasks.schedule('backup-scheduler',
                           TaskCreator.create_project_backup_scheduler_task(
                               project_id,
                               next_page_token)
                           )

    @staticmethod
    def _schedule_dataset_backup_scheduler_tasks(project_id, dataset_ids):
        logging.info(
            u'Scheduling Dataset Backup Scheduler tasks for %s %s project datasets: %s.',
            len(dataset_ids), project_id, dataset_ids)

        tasks = []

        for dataset_id in dataset_ids:
            tasks.append(
                TaskCreator.create_dataset_backup_scheduler_task(
                    project_id=project_id,
                    dataset_id=dataset_id))

        Tasks.schedule('backup-scheduler', tasks)
