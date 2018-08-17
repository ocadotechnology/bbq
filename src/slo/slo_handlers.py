import logging

import webapp2

from src.commons.config.configuration import configuration
from src.slo.x_days_sli.x_days_sli_service import XDaysSLIService


class XDaysSLIHandler(webapp2.RequestHandler):

    def get(self):
        logging.info("TODO Recalculating SLI has been started.")
        sli_service = XDaysSLIService()
        sli_service.calculate_sli()


app = webapp2.WSGIApplication([
    ('/cron/slo/calculate', XDaysSLIHandler)
], debug=configuration.debug_mode)
