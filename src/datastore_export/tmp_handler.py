import webapp2

from src.configuration import configuration
from src.datastore_export.gcs_to_bq_export_service import GCSToBQExportService


class TmpHandler(webapp2.RequestHandler):

  def __init__(self, request=None, response=None):
    super(TmpHandler, self).__init__(request, response)
    pass

  def get(self):  # nopep8 pylint: disable=R0201
    export_service = GCSToBQExportService()
    export_service.make_export()


app = webapp2.WSGIApplication([webapp2.Route(
    '/export_gcs_to_bq',
    TmpHandler
)], debug=configuration.debug_mode)