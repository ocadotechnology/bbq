import webapp2

from src.commons.config.configuration import configuration
from src.commons.handlers.json_request_helper import JsonRequestHelper
from src.slo.backup_creation_latency.latency_violation_sli_service import LatencyViolationSliService


class LatencyViolationSliHandler(webapp2.RequestHandler):
    def post(self):
        json_data = JsonRequestHelper.parse_request_body(self.request.body)
        json_table = json_data['table']
        x_days = json_data['x_days']
        LatencyViolationSliService(x_days).check_and_stream_violation(json_table)


app = webapp2.WSGIApplication([
    ('/sli/latency/violation', LatencyViolationSliHandler)
], debug=configuration.debug_mode)
