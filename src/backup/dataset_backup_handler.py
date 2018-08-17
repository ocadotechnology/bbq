import logging

import webapp2

from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
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
        logging.info('Backing up dataset: ' + dataset_id)
        self.BQ.for_each_table(
            project_id=project_id,
            dataset_id=dataset_id,
            func=self.schedule_backup_task
        )

    # pylint: disable=R0201
    def schedule_backup_task(self, projectId, datasetId, tableId):
        logging.info("Schedule_backup_task: '%s:%s.%s'",
                     projectId, datasetId, tableId)
        task = Tasks.create(
            method='GET',
            url='/tasks/backups/table/{0}/{1}/{2}'
            .format(projectId, datasetId, tableId))
        Tasks.schedule('backup-worker', task)


app = webapp2.WSGIApplication([
    ('/tasks/backups/dataset', DatasetBackupHandler)
], debug=configuration.debug_mode)
