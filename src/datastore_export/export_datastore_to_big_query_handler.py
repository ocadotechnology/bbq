import datetime
import json
import logging

import webapp2
from google.appengine.api.app_identity import app_identity

from src.commons.config.configuration import configuration
from src.datastore_export.export_datastore_to_big_query_service import \
    ExportDatastoreToBigQueryService


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        date = now.split("_")[0]

        gcs_output_uri = self.__create_gcs_output_url(now)
        kinds = self.request.get_all('kind')

        logging.info("Scheduling export of Datastore backups to Big Query...")
        service = ExportDatastoreToBigQueryService(date)

        finished_with_success = service.export(gcs_output_uri, kinds)

        self.__parse_result(finished_with_success)

    def __parse_result(self, finished_with_success):
        http_status = 200 if finished_with_success else 500
        response_status = "success" if finished_with_success else "failed"

        self.response.set_status(http_status)
        self.response.out.write(json.dumps({'status': response_status}))
        self.response.headers['Content-Type'] = 'application/json'

        if finished_with_success:
            logging.info("Export of DS entities to BQ finished successfully.")
        else:
            logging.warning(
                "Export of DS entities to BQ finished with some warnings.")

    @staticmethod
    def __create_gcs_output_url(gcs_folder_name):
        app_id = app_identity.get_application_id()
        output_url_prefix = "gs://staging.{}.appspot.com/{}" \
            .format(app_id, gcs_folder_name)
        return output_url_prefix


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
