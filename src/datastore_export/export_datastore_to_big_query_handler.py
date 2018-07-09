import logging

import webapp2

from src.configuration import configuration
from src.datastore_export.export_datastore_to_gcs_service import \
    ExportDatastoreToGCSService
from src.datastore_export.export_gcs_to_big_query_service import \
    ExportGCSToBigQueryService


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        result = ExportDatastoreToGCSService\
            .invoke(self.request, self.response)\
            .wait_till_done(timeout=600)

        if not result.is_finished_with_success():
            raise Exception("ExportDatastoreToBigQueryHandler NOT DONE !!!")

        ExportGCSToBigQueryService.export(result.get_output_url_prefix())

        logging.info("SUCESS !!!")


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
