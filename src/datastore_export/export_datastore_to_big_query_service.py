import logging

from google.appengine.api.app_identity import app_identity

from src.datastore_export.export_datastore_backups_to_gcs_service import \
    ExportDatastoreBackupsToGCSService
from src.datastore_export.load_datastore_backups_to_big_query_service import \
    LoadDatastoreBackupsToBigQueryService


class ExportDatastoreToBigQueryService(object):
    def __init__(self, date):
        self.export_service = ExportDatastoreBackupsToGCSService()
        self.load_service = LoadDatastoreBackupsToBigQueryService(date)

    def export(self, gcs_output_uri, kinds):
        logging.info("Scheduling export of Datastore entities to GCS ...")
        export_result = self.export_service.export(gcs_output_uri, kinds)

        logging.info("Loading Datastore backups from GCS to Big Query")
        load_result = self.load_service.load(gcs_output_uri, kinds)

        return export_result and load_result

    @staticmethod
    def __create_gcs_output_url(gcs_folder_name):
        app_id = app_identity.get_application_id()
        output_url_prefix = "gs://staging.{}.appspot.com/{}" \
            .format(app_id, gcs_folder_name)
        return output_url_prefix
