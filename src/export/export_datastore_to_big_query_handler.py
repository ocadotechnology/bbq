import webapp2

from src.configuration import configuration
from src.export.export_datastore_to_gcs import ExportDatastoreToGCS


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        is_done = ExportDatastoreToGCS\
            .invoke(self.request, self.response)\
            .wait_till_done(timeout=600)

        if not is_done:
            raise Exception("ExportDatastoreToBigQueryHandler NOT DONE !!!")


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
