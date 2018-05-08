import webapp2

from src.environment import Environment
from src.backup.backup_scheduler import BackupScheduler
from src.bbq_authenticated_handler import BbqAuthenticatedHandler


class OrganizationBackupHandler(webapp2.RequestHandler):

    def get(self):
        backup_scheduler = BackupScheduler()
        backup_scheduler.iterate_over_all_datasets_and_schedule_backups()


class OrganizationBackupAuthenticatedHandler(BbqAuthenticatedHandler,
                                             OrganizationBackupHandler):

    def __init__(self, request=None, response=None):
        super(OrganizationBackupAuthenticatedHandler, self). \
            __init__(request, response)


app = webapp2.WSGIApplication([
    ('/cron/backup', OrganizationBackupHandler),
    ('/backups/schedule', OrganizationBackupAuthenticatedHandler)
], debug=Environment.is_debug_mode_allowed())
