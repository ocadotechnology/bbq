import json
import logging

from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.copy_job_async.copy_job.copy_job_request import CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job_result import CopyJobResult
from src.commons.big_query.copy_job_async.task_creator import TaskCreator


class ResultCheck(object):
    __MAX_RETRY_COUNT = 6

    def __init__(self):
        self.BQ = BigQuery()

    def check(self, result_check_request):
        self.__copy_job_type_id = result_check_request.copy_job_type_id
        self.__post_copy_action_request = result_check_request.post_copy_action_request
        job_json = self.BQ.get_job(result_check_request.job_reference)

        logging.info('Checking result (retryCount=%s) of job: %s',
                     result_check_request.retry_count, json.dumps(job_json))

        copy_job_result = CopyJobResult(job_json)

        if copy_job_result.is_done():
            logging.info('Copy job %s complete',
                         result_check_request.job_reference)
            self.__process_copy_job_result(
                copy_job_result,
                result_check_request.retry_count
            )
        else:
            logging.info(
                "Copy job '%s' not completed yet. Another result check "
                "is put on the queue.",
                result_check_request.job_reference)
            TaskCreator.create_copy_job_result_check(result_check_request)

    def __process_copy_job_result(self, job_result, retry_count):
        if job_result.has_errors():
            logging.info("retry_count: %s", retry_count)
            logging.error(job_result.error_message)
            if self.__should_retry(job_result.error_result) \
                    and retry_count < self.__MAX_RETRY_COUNT:

                logging.error('We may need to re-trigger this task.')
                retry_count += 1
                TaskCreator.create_copy_job(
                    CopyJobRequest(
                        task_name_suffix=None,
                        copy_job_type_id=self.__copy_job_type_id,
                        source_big_query_table=job_result.source_bq_table,
                        target_big_query_table=job_result.target_bq_table,
                        create_disposition=job_result.create_disposition,
                        write_disposition=job_result.write_disposition,
                        retry_count=retry_count,
                        post_copy_action_request=self.__post_copy_action_request
                    )
                )
                return

        if self.__post_copy_action_request is not None:
            TaskCreator.create_post_copy_action(
                copy_job_type_id=self.__copy_job_type_id,
                post_copy_action_request=self.__post_copy_action_request,
                job_json=job_result.get_raw_job_json()
            )

    @staticmethod
    def __should_retry(error_result):
        reason = error_result['reason']
        if reason == 'backendError':  # BigQuery error, retry
            return True
        if reason == 'internalError':  # BigQuery error, retry
            return True
        if reason == 'quotaExceeded':  # copy jobs quota exceeded
            return True
        if reason == 'duplicate':  # table exists already
            return False
        if reason == 'invalid':  # invalid table type, check it
            return False
        return False
