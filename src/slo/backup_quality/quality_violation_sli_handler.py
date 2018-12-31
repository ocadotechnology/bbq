import webapp2

from src.commons.config.configuration import configuration
from src.commons.handlers.json_request_helper import JsonRequestHelper
from src.slo.backup_quality.quality_violation_sli_service import QualityViolationSliService


class ViolationSliHandler(webapp2.RequestHandler):
    def post(self):
        json_data = JsonRequestHelper.parse_request_body(self.request.body)
        json_table = json_data['table']
        QualityViolationSliService().check_and_stream_violation(json_table)


app = webapp2.WSGIApplication([
    ('/sli/quality/violation', ViolationSliHandler)
], debug=configuration.debug_mode)
