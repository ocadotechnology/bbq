import datetime
import json
import logging

import webapp2

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

        logging.info("Scheduling export of Datastore entities to GCS ...")
        ExportDatastoreBackupsToGCSService().export(gcs_output_uri, kinds)

        logging.info("Loading Datastore backups from GCS to Big Query")
        LoadDatastoreBackupsToBigQueryService(now_date)\
            .load(gcs_output_uri, kinds)

        logging.info(
            "Export of Datastore entities to Big Query finished successfully."
        )

        self.response.headers['Content-Type'] = 'application/json'
        self.response.set_status(200)
        self.response.out.write(json.dumps({'status': 'success'}))

    @staticmethod
    def __create_gcs_output_url(gcs_folder_name):
        app_id = configuration.backup_project_id
        output_url_prefix = "gs://staging.{}.appspot.com/{}" \
            .format(app_id, gcs_folder_name)
        return output_url_prefix


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-big-query',
                  ExportDatastoreToBigQueryHandler)
], debug=configuration.debug_mode)
