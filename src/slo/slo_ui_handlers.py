import webapp2
from src.commons.handlers.common_handlers import BaseHandler

from src.commons.config.configuration import configuration


class MainPageHandler(BaseHandler):
    def get(self):
        self.render_response('index_slo.html')


app = webapp2.WSGIApplication([
    ('/', MainPageHandler),
    ('/_ah/start', MainPageHandler)
], debug=configuration.debug_mode)
