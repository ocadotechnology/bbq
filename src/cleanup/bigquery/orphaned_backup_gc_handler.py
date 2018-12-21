import logging
import webapp2

from src.commons.config.configuration import configuration
from src.cleanup.bigquery.orphaned_backup_gc import OrphanedBackupGC


class OrphanedBackupGCHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(OrphanedBackupGCHandler, self).__init__(request, response)
        self.orphaned_backup_gc = OrphanedBackupGC()

    def delete(self):
        logging.info('Cleanup of orphaned backups from bigQuery for project: {0}'
                     .format(configuration.backup_project_id))
        self.orphaned_backup_gc.cleanup_orphaned_backups()


app = webapp2.WSGIApplication([
    ('/cleanup/orphaned/backup', OrphanedBackupGCHandler)
], debug=configuration.debug_mode)
