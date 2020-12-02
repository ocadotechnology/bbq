import logging

import webapp2

from src.backup.scheduler.partitioned_table.partitioned_table_backup_scheduler import \
    PartitionedTableBackupScheduler
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class PartitionedTableBackupSchedulerHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(PartitionedTableBackupSchedulerHandler, self).__init__(request,
                                                                     response)

        self.BQ = BigQuery()

        # now let's check if this task is not a retry of some previous (which
        # failed for some reason) if so - let's log when it hits the defined
        # mark so we can catch it on monitoring:
        Tasks.log_task_metadata_for(request=self.request)

    def post(self):
        project_id = self.request.get('projectId')
        dataset_id = self.request.get('datasetId')
        table_id = self.request.get('tableId')

        logging.info(u'Partitioned Table Backup Scheduler task for %s:%s.%s',
                     project_id, dataset_id, table_id)
        PartitionedTableBackupScheduler().schedule_backup(project_id=project_id,
                                                          dataset_id=dataset_id,
                                                          table_id=table_id)


app = webapp2.WSGIApplication([
    ('/tasks/schedulebackup/partitionedtable',
     PartitionedTableBackupSchedulerHandler)
], debug=configuration.debug_mode)
