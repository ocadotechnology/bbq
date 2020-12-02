import logging

import webapp2

from src.backup.scheduler.project.project_backup_scheduler import \
    ProjectBackupScheduler
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class ProjectBackupSchedulerHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(ProjectBackupSchedulerHandler, self).__init__(request, response)

        self.BQ = BigQuery()

        # now let's check if this task is not a retry of some previous (which
        # failed for some reason) if so - let's log when it hits the defined
        # mark so we can catch it on monitoring:
        Tasks.log_task_metadata_for(request=self.request)

    def post(self):
        project_id = self.request.get('projectId')
        page_token = self.request.get('pageToken', None)
        logging.info(
            u'Project Backup Scheduler task for %s, page_token: %s',
            project_id, page_token)
        ProjectBackupScheduler().schedule_backup(project_id=project_id,
                                                 page_token=page_token)


app = webapp2.WSGIApplication([
    ('/tasks/schedulebackup/project', ProjectBackupSchedulerHandler)
], debug=configuration.debug_mode)
