import logging
import time

from src.appinfo import AppInfo
from src.big_query.big_query import BigQuery
from src.configuration import configuration
from src.error_reporting import ErrorReporting

DATASET_ID = "datastore_export"
TIMEOUT = 600
PERIOD = 60


class LoadDatastoreBackupsToBigQueryException(Exception):
    pass


class LoadDatastoreBackupsToBigQueryService(object):

    def __init__(self, date):
        self.date = date
        self.big_query = BigQuery()
        self.location = AppInfo().get_location()

    def load(self, source_uri, kinds):
        self.big_query.create_dataset(
            configuration.backup_project_id,
            DATASET_ID, self.location
        )

        load_job_ids = []
        for kind in kinds:
            job_id = self.big_query.insert_job(
                project_id=configuration.backup_project_id,
                body=self.__create_job_body(source_uri, kind)
            )
            load_job_ids.append(job_id)

        return self.__all_finished_with_success(load_job_ids)

    def __create_job_body(self, source_uri, kind):
        return {
            "projectId": configuration.backup_project_id,
            "location": self.location,
            "configuration": {
                "load": {
                    "sourceFormat": "DATASTORE_BACKUP",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "sourceUris": [
                        "{}/all_namespaces/kind_{}/all_namespaces_kind_{}"
                        ".export_metadata".format(source_uri, kind, kind)
                    ],
                    "destinationTable": {
                        "projectId": configuration.backup_project_id,
                        "datasetId": DATASET_ID,
                        "tableId": kind + "_" + self.date
                    }
                }
            }
        }

    def __all_finished_with_success(self, load_job_ids):
        result = True
        for load_job_id in load_job_ids:
            if not self.__is_finished_with_success(load_job_id):
                result = False
        return result

    def __is_finished_with_success(self, load_job_id):
        finish_time = time.time() + TIMEOUT
        self.__wait_till_done(load_job_id)

        if time.time() < finish_time:
            ErrorReporting().report(
                "Timeout (%d seconds) exceeded !!!" % TIMEOUT)
            return False
        return True

    def __wait_till_done(self, load_job_id):
        while True:
            logging.info(
                "Loading Datastore backups from GCS to BQ (jobId: %s) - "
                "waiting %d seconds for request to end...", load_job_id, PERIOD
            )
            time.sleep(PERIOD)

            result = self.big_query.get_job(
                project_id=configuration.backup_project_id,
                job_id=load_job_id
            )

            if 'errors' in result['status']:
                raise LoadDatastoreBackupsToBigQueryException(
                    "Export from GCS to BQ failed, job id: {}".format(
                        load_job_id)
                )
            if result['status']['state'] == 'DONE':
                logging.info("Export from GCS to BQ finished successfully.")
                return
            logging.info("Export from GCS to BQ still in progress ...")
