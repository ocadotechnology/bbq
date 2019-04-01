import logging
import uuid

from apiclient.errors import HttpError

from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_job_error import BigQueryJobError
from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.decorators.retry import retry


class CopyJobService(object):

    @staticmethod
    def run_copy_job_request(copy_job_request):
        source_big_query_table = copy_job_request.source_big_query_table
        target_big_query_table = copy_job_request.target_big_query_table

        copy_job_result = CopyJobService.__schedule(
            source_big_query_table=source_big_query_table,
            target_big_query_table=target_big_query_table,
            job_id=CopyJobService._create_random_job_id(),
            create_disposition=copy_job_request.create_disposition,
            write_disposition=copy_job_request.write_disposition)

        copy_job_result.create_post_copy_action(copy_job_request)

    @staticmethod
    @retry(Exception, tries=6, delay=2, backoff=2)
    def __schedule(source_big_query_table, target_big_query_table, job_id,
                   create_disposition, write_disposition):
        logging.info("Scheduling job ID: " + job_id)
        target_project_id = target_big_query_table.get_project_id()
        job_data = {
            "jobReference": {
                "jobId": job_id,
                "projectId": target_project_id
            },
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source_big_query_table.get_project_id(),
                        "datasetId": source_big_query_table.get_dataset_id(),
                        "tableId": source_big_query_table.get_table_id(),
                    },
                    "destinationTable": {
                        "projectId": target_project_id,
                        "datasetId": target_big_query_table.get_dataset_id(),
                        "tableId": target_big_query_table.get_table_id(),
                    },
                    "createDisposition": create_disposition,
                    "writeDisposition": write_disposition
                }
            }
        }
        try:
            job_reference = BigQuery().insert_job(target_project_id, job_data)
            logging.info("Successfully insert: %s", job_reference)
            return job_reference
        except HttpError as bq_error:
            copy_job_error = BigQueryJobError(bq_error,
                                              source_big_query_table,
                                              target_big_query_table)
            if copy_job_error.is_deadline_exceeded():
                job_json = CopyJobService.__get_job(job_id, target_project_id,
                                                    copy_job_error.location)
                return CopyJobService.__to_bq_job_reference(job_json)
            elif copy_job_error.should_be_retried():
                logging.warning(copy_job_error)
                return BigQueryJobReference(
                    project_id=target_project_id,
                    job_id=job_id,
                    location=BigQueryTableMetadata.get_table_by_big_query_table(
                        source_big_query_table).get_location())
            else:
                logging.exception(copy_job_error)
                return copy_job_error
        except Exception as error:
            logging.error("%s Exception thrown during Copy Job creation: %s",
                          type(error), error)
            raise error

    @staticmethod
    @retry(HttpError, tries=6, delay=2, backoff=2)
    def __get_job(job_id, project_id, location):
        job_reference = BigQueryJobReference(project_id=project_id,
                                             job_id=job_id,
                                             location=location)
        return BigQuery().get_job(job_reference)

    @staticmethod
    def __to_bq_job_reference(job_json):
        job_reference = job_json["jobReference"]
        return BigQueryJobReference(job_reference["projectId"],
                                    job_reference["jobId"],
                                    job_reference["location"])

    @staticmethod
    def _create_random_job_id():
        return "bbq_copy_job_" + str(uuid.uuid4())
