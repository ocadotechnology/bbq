import logging
import uuid

import webapp2

from src.backup.scheduler.organization.organization_backup_scheduler import \
    OrganizationBackupScheduler
from src.commons import request_correlation_id
from src.commons.config.configuration import configuration


class OrganizationBackupSchedulerHandler(webapp2.RequestHandler):

    def get(self):
        page_token = self.request.get('pageToken', None)
        if not request_correlation_id.get():
            request_correlation_id.set_correlation_id(str(uuid.uuid4()))

        logging.info(u'Organisation Backup Scheduler task for page_token: %s',
                     page_token)
        OrganizationBackupScheduler().schedule_backup(page_token=page_token)


app = webapp2.WSGIApplication([
    ('/cron/backup', OrganizationBackupSchedulerHandler),
    ('/tasks/schedulebackup/organization',
     OrganizationBackupSchedulerHandler)
], debug=configuration.debug_mode)
