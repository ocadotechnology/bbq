import webapp2

from src.commons.config.configuration import configuration
from src.slo.x_days_sli.x_days_sli_service import XDaysSLIService


class CalculateXDaysSLIHandler(webapp2.RequestHandler):

    def post(self):
        x_days = self.request.get('x_days', None)
        XDaysSLIService(x_days).recalculate_sli()


app = webapp2.WSGIApplication([
    ('/slo/recalculate_x_days', CalculateXDaysSLIHandler)
], debug=configuration.debug_mode)
