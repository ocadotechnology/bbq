import webapp2
from src.common_handlers import BaseHandler

from src.configuration import configuration


class MainSloPage(BaseHandler):
    def get(self):
        self.render_response('index_slo.html')


app = webapp2.WSGIApplication([
    ('/', MainSloPage),
    ('/_ah/start', MainSloPage),
], debug=configuration.debug_mode)
