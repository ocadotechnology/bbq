import logging

import webapp2

from src.backup.scheduler.dataset.dataset_backup_scheduler import \
    DatasetBackupScheduler
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class DatasetBackupSchedulerHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(DatasetBackupSchedulerHandler, self).__init__(request, response)

        self.BQ = BigQuery()

        # now let's check if this task is not a retry of some previous (which
        # failed for some reason) if so - let's log when it hits the defined
        # mark so we can catch it on monitoring:
        Tasks.log_task_metadata_for(request=self.request)

    def post(self):
        project_id = self.request.get('projectId')
        dataset_id = self.request.get('datasetId')
        page_token = self.request.get('pageToken', None)

        logging.info(u'Dataset Backup Scheduler task for %s:%s, page_token: %s',
                     project_id, dataset_id, page_token)
        DatasetBackupScheduler().schedule_backup(project_id=project_id,
                                                 dataset_id=dataset_id,
                                                 page_token=page_token)


app = webapp2.WSGIApplication([
    ('/tasks/schedulebackup/dataset', DatasetBackupSchedulerHandler)
], debug=configuration.debug_mode)
