import webapp2

from src.commons.handlers.common_handlers import BaseHandler
from src.commons.config.configuration import configuration


class MainPage(BaseHandler):
    def get(self):
        self.render_response('index.html')


class RestoreDatasetUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreDataset.html',
                             default_restoration_project_id=
                             configuration.default_restoration_project_id)


class RestoreListUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreList.html',
                             default_restoration_project_id=
                             configuration.default_restoration_project_id)


class RestoreTableUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreTable.html',
                             default_restoration_project_id=
                             configuration.default_restoration_project_id)


class OnDemandTableBackupUIHandler(BaseHandler):
    def get(self):
        self.render_response('on_demand_table_backup.html')


app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/_ah/start', MainPage),
    ('/ui/restoreDataset', RestoreDatasetUIHandler),
    ('/ui/restoreList', RestoreListUIHandler),
    ('/ui/restoreTable', RestoreTableUIHandler),
    ('/ui/onDemandTableBackup', OnDemandTableBackupUIHandler)
], debug=configuration.debug_mode)
