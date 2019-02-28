import logging
import time

import googleapiclient.discovery
from google.appengine.api.app_identity import app_identity
from oauth2client.client import GoogleCredentials

from src.commons.error_reporting import ErrorReporting

# 30 minutes
TIMEOUT = 3 * 600
# 1 minute
PERIOD = 60


class ExportDatastoreToGCSException(Exception):
    pass


class ExportDatastoreBackupsToGCSService(object):

    def __init__(self):
        self.app_id = app_identity.get_application_id()
        self.service = googleapiclient.discovery.build(
            'datastore',
            'v1',
            credentials=self.__create_credentials(),
            http=self.__create_http(),
        )

    @staticmethod
    def __create_credentials():
        return GoogleCredentials.get_application_default()

    @staticmethod
    def __create_http():
        return None

    def export(self, gcs_output_url, kinds):
        body = {
            'output_url_prefix': gcs_output_url,
            'entity_filter': {
                'kinds': kinds
            }
        }
        response = self.service \
            .projects() \
            .export(projectId=self.app_id, body=body) \
            .execute()

        return self.__is_finished_with_success(response)

    def __is_finished_with_success(self, response):
        finish_time = time.time() + TIMEOUT
        self.__wait_till_done(response["name"])

        if time.time() > finish_time:
            ErrorReporting().report(
                "Timeout (%d seconds) exceeded !!!" % TIMEOUT)
            logging.warning("Export from DS to GCS finished with timeout.")
            return False
        logging.info("Export from DS to GCS finished successfully.")
        return True

    def __wait_till_done(self, operation_id):
        while True:
            logging.info("Export from DS to GCS - "
                         "waiting %d seconds for request to end...", PERIOD)
            time.sleep(PERIOD)

            response = self.service \
                .projects() \
                .operations() \
                .get(name=operation_id) \
                .execute()
            logging.info(response)

            if "error" in response:
                error = response["error"]
                error_message = "Request finished with errors: %s" % error
                raise ExportDatastoreToGCSException(error_message)
            if response.get("done"):
                return
            logging.info("Export from DS to GCS still in progress ...")
