import webapp2

from src.commons.config.configuration import configuration
from src.slo.backup_quality.quality_sli_service import QualitySliService


class QualitySliHandler(webapp2.RequestHandler):
    def post(self):
        QualitySliService().recalculate_sli()


app = webapp2.WSGIApplication([
    ('/sli/quality', QualitySliHandler),
], debug=configuration.debug_mode)
