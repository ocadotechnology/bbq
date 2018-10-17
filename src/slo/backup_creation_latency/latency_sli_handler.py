import webapp2

from src.commons.config.configuration import configuration
from src.slo.backup_creation_latency.latency_sli_service import LatencySliService


class LatencySliHandler(webapp2.RequestHandler):

    def post(self):
        x_days = self.request.get('x_days', None)
        LatencySliService(x_days).recalculate_sli()


app = webapp2.WSGIApplication([
    ('/sli/latency_for_x_days', LatencySliHandler)
], debug=configuration.debug_mode)
