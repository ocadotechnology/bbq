import webapp2

from src.commons.config.configuration import configuration
from src.slo.backup_quality.quality_sli_service import QualitySliService


class QualitySliHandler(webapp2.RequestHandler):
    def post(self):
        QualitySliService().recalculate_sli()


class ViolationSliHandler(webapp2.RequestHandler):
    def post(self):
        QualitySliService().check_and_stream_violation(self.request.json['table'])


app = webapp2.WSGIApplication([
    ('/sli/quality', QualitySliHandler),
    ('/sli/quality/violation', ViolationSliHandler)
], debug=configuration.debug_mode)
