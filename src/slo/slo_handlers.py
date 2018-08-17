import logging

import webapp2

from src.commons.config.configuration import configuration


class XDaysSLIHandler(webapp2.RequestHandler):

    def get(self):
        logging.info("TODO Recalculating SLI has been started.")


app = webapp2.WSGIApplication([
    ('/cron/slo/calculate', XDaysSLIHandler)
], debug=configuration.debug_mode)
