import logging

from src.backup.scheduler.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class OrganizationBackupScheduler(object):

    def __init__(self):
        self.big_query = BigQuery()
        self.custom_projects_list = configuration.backup_settings_custom_project_list
        self.projects_to_skip = configuration.projects_to_skip

    def schedule_backup(self, page_token=None):
        if self.custom_projects_list:
            self._schedule_project_backup_scheduler_for_custom_project_list()
            return

        projects_ids_to_backup, next_page_token = self.big_query.list_project_ids(
            page_token=page_token)

        self._schedule_project_backup_scheduler_tasks(projects_ids_to_backup)

        if next_page_token:
            logging.info(
                u'Scheduling Organisation Backup Scheduler task for page_token: %s',
                next_page_token)
            Tasks.schedule('backup-scheduler',
                           TaskCreator.create_organisation_backup_scheduler_task(
                               page_token=next_page_token))

    def _schedule_project_backup_scheduler_tasks(self, project_ids):
        logging.info(
            u'Scheduling Project Backup Scheduler tasks for %s projects: %s',
            len(project_ids), project_ids)

        tasks = []

        for project_id in project_ids:

            if project_id not in self.projects_to_skip:
                tasks.append(
                    TaskCreator.create_project_backup_scheduler_task(
                        project_id=project_id
                    )
                )
            else:
                logging.info(u'Project %s is skipped.', project_id)

        Tasks.schedule('backup-scheduler', tasks)

    def _schedule_project_backup_scheduler_for_custom_project_list(self):
        logging.info(
            u'Custom project list is defined. Only projects defined in configuration will be scheduled for backup')
        self._schedule_project_backup_scheduler_tasks(self.custom_projects_list)
