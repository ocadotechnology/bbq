import webapp2

from src.backup.backup_scheduler import BackupScheduler
from src.commons.config.configuration import configuration


class OrganizationBackupHandler(webapp2.RequestHandler):

    def get(self):
        backup_scheduler = BackupScheduler()
        backup_scheduler.iterate_over_all_datasets_and_schedule_backups()


app = webapp2.WSGIApplication([
    ('/cron/backup', OrganizationBackupHandler)
], debug=configuration.debug_mode)
