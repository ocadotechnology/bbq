import logging

import webapp2
from apiclient.errors import HttpError

from src.backup.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.decorators.retry import retry
from src.commons.tasks import Tasks


class DatasetBackupHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(DatasetBackupHandler, self).__init__(request, response)

        self.BQ = BigQuery()

        # now let's check if this task is not a retry of some previous (which
        # failed for some reason) if so - let's log when it hits the defined
        # mark so we can catch it on monitoring:
        Tasks.log_task_metadata_for(request=self.request)

    def post(self):
        project_id = self.request.get('projectId')
        dataset_id = self.request.get('datasetId')
        self._schedule_backup_tasks(project_id, dataset_id)

    @retry(HttpError, tries=3, delay=2, backoff=2)
    def _schedule_backup_tasks(self, project_id, dataset_id):
        logging.info("Backing up dataset: '%s:%s' ", project_id, dataset_id)
        TaskCreator.schedule_tasks_for_tables_backup(
            project_id,
            dataset_id,
            self.BQ.list_table_ids(project_id, dataset_id))


app = webapp2.WSGIApplication([
    ('/tasks/backups/dataset', DatasetBackupHandler)
], debug=configuration.debug_mode)
