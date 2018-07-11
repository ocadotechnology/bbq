import datetime
import json
import logging

import webapp2
from google.appengine.api.app_identity import app_identity

from src.configuration import configuration
from src.datastore_export.export_datastore_backups_to_gcs_service import \
    ExportDatastoreBackupsToGCSService
from src.datastore_export.load_datastore_backups_to_big_query_service import \
    LoadDatastoreBackupsToBigQueryService


class ExportDatastoreToBigQueryHandler(webapp2.RequestHandler):
    def get(self):
        now_datetime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        now_date = now_datetime.split("_")[0]

        kinds = self.request.get_all('kind')
        gcs_output_uri = self.__create_gcs_output_url(now_datetime)

        export_ds_to_gcs_service = ExportDatastoreBackupsToGCSService()
        load_ds_from_gcs_to_bq_service = \
            LoadDatastoreBackupsToBigQueryService(now_date)

        finished_with_success = True

        logging.info("Scheduling export of Datastore entities to GCS ...")
        result = export_ds_to_gcs_service.export(gcs_output_uri, kinds)
        if not result:
            finished_with_success = False

        logging.info("Loading Datastore backups from GCS to Big Query")
        result = load_ds_from_gcs_to_bq_service.load(gcs_output_uri, kinds)
        if not result:
            finished_with_success = False

        logging.info("Export of DS entities to BQ finished successfully.")
        response_status = "success" if finished_with_success else "failed"
        http_status_code = 200 if finished_with_success else 500

        self.response.headers['Content-Type'] = 'application/json'
        self.response.set_status(http_status_code)
        self.response.out.write(json.dumps({'status': response_status}))

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
