import logging
import uuid

from src.commons import request_correlation_id
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.error_reporting import ErrorReporting
from src.commons.tasks import Tasks


class BackupScheduler(object):

    def __init__(self):
        self.big_query = BigQuery()
        self.request_correlation_id = str(uuid.uuid4())

    def iterate_over_all_datasets_and_schedule_backups(self):
        custom_project_list = configuration.backup_settings_custom_project_list
        if custom_project_list:
            project_ids = custom_project_list
            logging.info('Only projects specified in the configuration will'
                         ' be backed up: %s', project_ids)
        else:
            project_ids = list(self.big_query.list_project_ids())

        logging.info('Scheduling backups of %s projects', len(project_ids))
        for project_id in project_ids:
            try:
                self.__list_and_backup_datasets(project_id)
            except Exception as ex:
                error_message = 'Failed to list and backup datasets: ' + str(ex)
                ErrorReporting().report(error_message)

    def __list_and_backup_datasets(self, project_id):
        if project_id in configuration.projects_to_skip:
            logging.info('Skipping project: %s', project_id)
            return

        logging.info('Backing up project: %s, request_correlation_id: %s',
                     project_id, self.request_correlation_id)
        for dataset_id in self.big_query.list_dataset_ids(project_id):
            try:
                self.__backup_dataset(project_id, dataset_id)
            except Exception as ex:
                error_message = 'Failed to backup dataset: ' + str(ex)
                ErrorReporting().report(error_message)

    def __backup_dataset(self, project_id, dataset_id):
        logging.info('Backing up dataset: %s', dataset_id)
        task = Tasks.create(
            url='/tasks/backups/dataset',
            params={
                'projectId': project_id,
                'datasetId': dataset_id
            },
            headers={
                request_correlation_id.HEADER_NAME:
                    self.request_correlation_id
            })
        Tasks.schedule('backup-scheduler', task)
