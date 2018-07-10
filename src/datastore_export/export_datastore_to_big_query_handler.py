import logging

import webapp2

from src.configuration import configuration
from src.datastore_export.export_datastore_to_gcs_service import \
    ExportDatastoreToGCSService
from src.datastore_export.export_gcs_to_big_query_service import \
    ExportGCSToBigQueryService


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        logging.info("Scheduling export of Datastore entities to GCS ...")
        output_url = ExportDatastoreToGCSService\
            .invoke(self.request, self.response)\
            .wait_till_done(timeout=600)

        logging.info("Scheduling export of GCS to Big Query")
        ExportGCSToBigQueryService.export(output_url)

        logging.info(
            "Export of Datastore entities to Big Query finished successfully."
        )


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
