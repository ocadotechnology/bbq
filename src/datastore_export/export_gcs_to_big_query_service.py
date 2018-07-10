import logging
import time

from src.big_query.big_query import BigQuery
from src.configuration import configuration
from src.error_reporting import ErrorReporting


class ExportGCSToBigQueryException(Exception):
    pass


class ExportGCSToBigQueryService(object):

    def __init__(self):
        self.big_query = BigQuery()

    def export(self, source_gcs_bucket):
        backup_load_job = self.create_load_job("Backup", source_gcs_bucket)
        table_load_job = self.create_load_job("Table", source_gcs_bucket)

        load_jobs = [backup_load_job, table_load_job]
        load_job_ids = []

        for load_job in load_jobs:
            job_id = self.big_query \
                .insert_job(configuration.backup_project_id, load_job)
            load_job_ids.append(job_id)

        for load_job_id in load_job_ids:
            self.__wait_till_done(load_job_id, 600)

    @classmethod
    def create_load_job(cls, entity, source_gcs_bucket):
        dataset_id, datetime = source_gcs_bucket.split("//")[1].split("/")
        date = datetime.split("_")[0]
        return {
            "projectId": configuration.backup_project_id,
            "location": "EU",
            "configuration": {
                "load": {
                    "sourceFormat": "DATASTORE_BACKUP",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "sourceUris": [
                        "{}/all_namespaces/kind_{}/"
                        "all_namespaces_kind_{}.export_metadata".format(
                            source_gcs_bucket, entity, entity)
                    ],
                    "destinationTable": {
                        "projectId": configuration.backup_project_id,
                        "datasetId": dataset_id,
                        "tableId": entity + "_" + date
                    }
                }
            }
        }

    @classmethod
    def __wait_till_done(cls, load_job_id, timeout, period=60):
        finish_time = time.time() + timeout
        while time.time() < finish_time:
            logging.info(
                "Export from GCS to BQ - "
                "waiting %d seconds for request to end...", period)
            time.sleep(period)

            result = BigQuery().get_job(
                project_id=configuration.backup_project_id,
                job_id=load_job_id
            )

            if 'errors' in result['status']:
                raise ExportGCSToBigQueryException(
                    "Export from GCS to BQ failed, job id: {}".format(
                        load_job_id)
                )
            if result['status']['state'] == 'DONE':
                logging.info("Export from GCS to BQ finished successfully.")
                return
            logging.info("Export from GCS to BQ still in progress ...")

        ErrorReporting().report("Timeout (%d seconds) exceeded !!!" % timeout)
