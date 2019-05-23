import logging
import time

from src.commons.config.appinfo import AppInfo
from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.error_reporting import ErrorReporting

DATASET_ID = "datastore_export"
# 30 minutes
TIMEOUT = 3 * 600
# 1 minute
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
            configuration.metadata_storage_project_id,
            DATASET_ID, self.location
        )

        load_jobs = []
        for kind in kinds:
            job_reference = self.big_query.insert_job(
                project_id=configuration.backup_project_id,
                body=self.__create_job_body(source_uri, kind)
            )
            load_jobs.append(job_reference)

        return self.__all_finished_with_success(load_jobs)

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
                        "projectId": configuration.metadata_storage_project_id,
                        "datasetId": DATASET_ID,
                        "tableId": kind + "_" + self.date
                    }
                }
            }
        }

    def __all_finished_with_success(self, load_jobs):
        result = True
        for load_job in load_jobs:
            if not self.__is_finished_with_success(load_job):
                result = False
        return result

    def __is_finished_with_success(self, load_job):
        finish_time = time.time() + TIMEOUT
        self.__wait_till_done(load_job)

        if time.time() > finish_time:
            ErrorReporting().report(
                "Timeout (%d seconds) exceeded !!!" % TIMEOUT)
            logging.warning("Export from GCS to BQ finished with timeout.")
            return False
        logging.info("Export from GCS to BQ finished successfully.")
        return True

    def __wait_till_done(self, load_job):
        while True:
            result = self.big_query.get_job(load_job)
            if 'errors' in result['status']:
                raise LoadDatastoreBackupsToBigQueryException(
                    "Export from GCS to BQ failed, job reference: {}".format(load_job)
                )
            if result['status']['state'] == 'DONE':
                return

            logging.info(
                "Export from GCS to BQ still in progress... %s Waiting %d seconds to check the results again.",
                load_job, PERIOD
            )
            time.sleep(PERIOD)
