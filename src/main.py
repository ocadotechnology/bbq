import webapp2

from src.common_handlers import BaseHandler
from src.configuration import configuration


class MainPage(BaseHandler):
    def get(self):
        self.render_response('index.html')


class RestoreDatasetUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreDataset.html',
                             restoration_project_id=
                             configuration.restoration_project_id)


class RestoreListUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreList.html',
                             restoration_project_id=
                             configuration.restoration_project_id)


class RestoreTableUIHandler(BaseHandler):
    def get(self):
        self.render_response('restoreTable.html',
                             restoration_project_id=
                             configuration.restoration_project_id)


app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/_ah/start', MainPage),
    ('/ui/restoreDataset', RestoreDatasetUIHandler),
    ('/ui/restoreList', RestoreListUIHandler),
    ('/ui/restoreTable', RestoreTableUIHandler)
], debug=configuration.debug_mode)
