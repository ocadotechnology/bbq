import webapp2

from src.backup.on_demand.on_demand_table_backup import OnDemandTableBackup
from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference
from src.commons.tasks import Tasks
from src.commons.handlers.json_handler import JsonHandler


class OnDemandTableBackupHandler(JsonHandler):
    def __init__(self, request=None, response=None):
        super(OnDemandTableBackupHandler, self).__init__(request, response)

        # now let's check if this task is not a retry of some previous (which
        # failed for some reason) if so - let's log when it hits the defined
        # mark so we can catch it on monitoring:
        Tasks.log_task_metadata_for(request=self.request)

    def get(self, project_id, dataset_id, table_id, partition_id=None): # nopep8 pylint: disable=R0201
        table_reference = TableReference(project_id, dataset_id,
                                         table_id, partition_id)
        OnDemandTableBackup.start(table_reference)


app = webapp2.WSGIApplication([
    webapp2.Route('/tasks/backups/on_demand/table/<project_id:[^/]+>/<dataset_id:'
                  '[^/]+>/<table_id:[^/]+>', OnDemandTableBackupHandler),
    webapp2.Route('/tasks/backups/on_demand/table/<project_id:[^/]+>/<dataset_id:'
                  '[^/]+>/<table_id:[^/]+>/<partition_id:[^/]+>',
                  OnDemandTableBackupHandler)
], debug=configuration.debug_mode)
