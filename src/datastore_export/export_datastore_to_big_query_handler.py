import datetime
import logging

import webapp2

from src.configuration import configuration
from src.datastore_export.export_datastore_to_gcs_service import \
    ExportDatastoreToGCSService
from src.datastore_export.export_gcs_to_big_query_service import \
    ExportGCSToBigQueryService


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        output_url = self.get_output_url_prefix(self.request)

        logging.info("Scheduling export of Datastore entities to GCS ...")
        ExportDatastoreToGCSService().export(output_url)

        logging.info("Scheduling export of GCS to Big Query")
        ExportGCSToBigQueryService().export(output_url)

        logging.info(
            "Export of Datastore entities to Big Query finished successfully."
        )

    @staticmethod
    def get_output_url_prefix(request):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_url_prefix = "gs://%s" % request.get('output_path')
        if '/' not in output_url_prefix[5:]:
            # Only a bucket name has been provided - no prefix or trailing slash
            output_url_prefix += '/' + timestamp
        else:
            output_url_prefix += timestamp
        return output_url_prefix


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
