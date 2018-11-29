import webapp2

from src.commons.config.configuration import configuration
from src.slo.backup_quality.quality_sli_service import QualitySliService
from src.commons.handlers.json_request_helper import JsonRequestHelper


class QualitySliHandler(webapp2.RequestHandler):
    def post(self):
        QualitySliService().recalculate_sli()


class ViolationSliHandler(webapp2.RequestHandler):
    def post(self):
        QualitySliService().check_and_stream_violation(
            JsonRequestHelper.parse_request_body(self.request.body))


app = webapp2.WSGIApplication([
    ('/sli/quality', QualitySliHandler),
    ('/sli/quality/violation', ViolationSliHandler)
], debug=configuration.debug_mode)
