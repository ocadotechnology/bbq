import logging

import webapp2
from src.commons.handlers.common_handlers import BaseHandler

from src.commons.config.configuration import configuration


class MainPageHandler(BaseHandler):

    def get(self):
        self.render_response('index_slo.html')


class XDaysSLIHandler(webapp2.RequestHandler):

    def get(self):
        logging.info("TODO Recalculating SLI has been started.")


app = webapp2.WSGIApplication([
    ('/', MainPageHandler),
    ('/_ah/start', MainPageHandler),
    ('/cron/slo/calculate', XDaysSLIHandler)
], debug=configuration.debug_mode)
