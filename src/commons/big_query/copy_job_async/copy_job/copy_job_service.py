import logging
import uuid

from apiclient.errors import HttpError

from src.commons.big_query.copy_job_async.result_check.result_check_request import \
    ResultCheckRequest
from src.commons.big_query.copy_job_async.task_creator import TaskCreator
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.decorators.retry import retry


class CopyJobService(object):

    @staticmethod
    def run_copy_job_request(copy_job_request):
        source_big_query_table = copy_job_request.source_big_query_table
        target_big_query_table = copy_job_request.target_big_query_table
        retry_count = copy_job_request.retry_count

        job_reference = CopyJobService.__schedule(
            source_big_query_table=source_big_query_table,
            target_big_query_table=target_big_query_table,
            job_id=CopyJobService._create_random_job_id(),
            create_disposition=copy_job_request.create_disposition,
            write_disposition=copy_job_request.write_disposition)

        if job_reference:
            TaskCreator.create_copy_job_result_check(
                ResultCheckRequest(
                    task_name_suffix=copy_job_request.task_name_suffix,
                    copy_job_type_id=copy_job_request.copy_job_type_id,
                    job_reference=job_reference,
                    retry_count=retry_count,
                    post_copy_action_request=copy_job_request.post_copy_action_request
                )
            )
        else:
            logging.error('Schedule of Copy Job from %s to %s failed' % (source_big_query_table, target_big_query_table))
            if copy_job_request.post_copy_action_request is not None:
                TaskCreator.create_post_copy_action(
                    copy_job_type_id=copy_job_request.copy_job_type_id,
                    post_copy_action_request=copy_job_request.post_copy_action_request,
                    job_json=CopyJobService.__generate_json_for_not_scheduled_job(source_big_query_table, target_big_query_table)
                )

    @staticmethod
    @retry(Exception, tries=6, delay=2, backoff=2)
    def __schedule(source_big_query_table, target_big_query_table, job_id,
                   create_disposition, write_disposition):
        logging.info("Scheduling job ID: " + job_id)
        job_data = {
            "jobReference": {
                "jobId": job_id,
                "projectId": target_big_query_table.get_project_id()
            },
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source_big_query_table.get_project_id(),
                        "datasetId": source_big_query_table.get_dataset_id(),
                        "tableId": source_big_query_table.get_table_id(),
                    },
                    "destinationTable": {
                        "projectId": target_big_query_table.get_project_id(),
                        "datasetId": target_big_query_table.get_dataset_id(),
                        "tableId": target_big_query_table.get_table_id(),
                    },
                    "createDisposition": create_disposition,
                    "writeDisposition": write_disposition
                }
            }
        }
        try:
            job_reference = BigQuery().insert_job(
                target_big_query_table.get_project_id(), job_data)
            logging.info("Successfully insert: %s", job_reference)
            return job_reference
        except HttpError as bq_error:
            if bq_error.resp.status == 403 and bq_error._get_reason().startswith('Access Denied'):
                    logging.exception('403 while creating Copy Job from %s to %s' % (source_big_query_table, target_big_query_table))
                    return None
            elif bq_error.resp.status == 404:
                logging.exception('404 while creating Copy Job from %s to %s' % (source_big_query_table, target_big_query_table))
                return None
            elif bq_error.resp.status == 409:
                logging.warning('409 while creating Copy Job from %s to %s' % (source_big_query_table, target_big_query_table))
                return BigQueryJobReference(
                    project_id=target_big_query_table.get_project_id(),
                    job_id=job_id,
                    location=BigQueryTableMetadata.get_table_by_big_query_table(source_big_query_table).get_location())
            else:
                raise
        except Exception as error:
            logging.error("%s Exception thrown during Copy Job creation: %s",
                          type(error), error)
            raise error

    @staticmethod
    def __generate_json_for_not_scheduled_job(source_table, target_table):
        return {
            "status": {
                "state": "DONE",
                "errors": [
                    {
                        "reason": "invalid",
                        "message": "Job not scheduled"
                    }
                ]
            },
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": source_table.get_project_id(),
                        "tableId": source_table.get_table_id(),
                        "datasetId": source_table.get_dataset_id()
                    },
                    "destinationTable": {
                        "projectId": target_table.get_project_id(),
                        "tableId": target_table.get_table_id(),
                        "datasetId": target_table.get_dataset_id()
                    }
                }
            }
        }

    @staticmethod
    def _create_random_job_id():
        return "bbq_copy_job_" + str(uuid.uuid4())
