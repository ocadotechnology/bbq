import webapp2
from webapp2_extras import jinja2

from src.configuration import configuration
from src.environment import Environment

VERSION = 'v1'


class BaseHandler(webapp2.RequestHandler):
  @webapp2.cached_property
  def jinja2(self):
    return jinja2.get_jinja2(app=self.app)

  def render_response(self, _template, **context):
    rv = self.jinja2.render_template(_template, version=VERSION, **context)
    self.response.write(rv)


class MainPage(BaseHandler):
  def get(self):
    domain = Environment.get_domain(configuration.backup_project_id)
    self.render_response('index.html', project_id=Environment.get_name(),
                         domain=domain)


class RestoreDatasetUIHandler(BaseHandler):
  def get(self):
    self.render_response('restoreDataset.html')


class RestoreListUIHandler(BaseHandler):
  def get(self):
    self.render_response('restoreList.html')


class RestoreTableUIHandler(BaseHandler):
  def get(self):
    self.render_response('restoreTable.html')


app = webapp2.WSGIApplication([
  ('/', MainPage),
  ('/_ah/start', MainPage),
  ('/ui/restoreDataset', RestoreDatasetUIHandler),
  ('/ui/restoreList', RestoreListUIHandler),
  ('/ui/restoreTable', RestoreTableUIHandler)
], debug=configuration.debug_mode)
