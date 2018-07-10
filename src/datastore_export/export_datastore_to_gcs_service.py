import logging
import time

import googleapiclient.discovery
import httplib2
from oauth2client.client import GoogleCredentials

from src.configuration import configuration
from src.error_reporting import ErrorReporting


class ExportDatastoreToGCSException(Exception):
    pass


class ExportDatastoreToGCSService(object):

    def __init__(self):
        self.http = self._create_http()
        self.service = googleapiclient.discovery.build(
            'datastore',
            'v1',
            credentials=GoogleCredentials.get_application_default(),
            http=self.http,
        )

    @staticmethod
    def _create_http():
        return httplib2.Http(timeout=60)

    def export(self, output_url_prefix, kinds):
        body = {
            'output_url_prefix': output_url_prefix,
            'entity_filter': {
                'kinds': kinds
            }
        }
        app_id = configuration.backup_project_id

        response = self.service.projects().export(projectId=app_id, body=body)
        self.__wait_till_done(response["name"], 600)

    def __wait_till_done(self, operation_id, timeout, period=60):
        finish_time = time.time() + timeout
        while time.time() < finish_time:
            logging.info("Export from DS to GCS - "
                         "waiting %d seconds for request to end...", period)
            time.sleep(period)

            result = self.service.projects().operations().get(operation_id)

            if "error" in result:
                error = result["error"]
                error_message = "Request finished with errors: %s" % error
                raise ExportDatastoreToGCSException(error_message)
            if result["done"]:
                logging.info("Export from DS to GCS finished successfully.")
                return
            logging.info("Export from DS to GCS still in progress ...")

        ErrorReporting().report("Timeout (%d seconds) exceeded !!!" % timeout)
