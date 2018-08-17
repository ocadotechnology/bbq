import json
import logging
import time

from src.commons.big_query.big_query import BigQuery


class RetriableCopyJobException(Exception):
    pass


class CopyJobService(object):

    def __init__(self, now):
        self.now = now
        self.BQ = BigQuery()

    # pylint: disable=R0201
    # Deprecated. Use Async version of copy job service which is more efficient
    def copy_table(
            self,
            source_big_query_table,
            target_big_query_table,
            write_disposition
    ):
        job_id = self.__schedule(source_big_query_table,
                                 target_big_query_table,
                                 write_disposition)
        self.__wait_till_done(target_big_query_table.get_project_id(), job_id)
        return self.__check_result(target_big_query_table.get_project_id(),
                                   target_big_query_table.get_dataset_id(),
                                   target_big_query_table.get_table_id(),
                                   job_id)

    def __schedule(self, source_big_query_table, target_big_query_table,
                   write_disposition):
        job_data = {
            "projectId": source_big_query_table.get_project_id(),
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
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": write_disposition
                }
            }
        }

        try:
            job_id = self.BQ.insert_job(
                target_big_query_table.get_project_id(),
                job_data
            )
            logging.info("Job ID: " + job_id)
        except Exception as error:
            logging.error("%s Exception thrown during Copy Job creation: %s",
                          type(error), error)
            raise error
        return job_id

    def __wait_till_done(self, project_id, job_id):
        while not self.__is_copy_job_done(project_id, job_id):
            logging.info('Waiting for the table to copy...')
            time.sleep(3)

    def __is_copy_job_done(self, project_id, job_id):
        job = self.__get_job(project_id, job_id)
        if job['status']['state'] == 'DONE':
            logging.info('Copy job %s complete', job_id)
            return True
        return False

    def __check_result(self, project_id, dataset_id, table_id, job_id):
        job = self.__get_job(project_id, job_id)
        if self.__has_errors(job):
            error = job['status']['errors'][0]
            if self.__should_retry(error):
                raise RetriableCopyJobException(
                    'Copy job finished with errors: {}. We may need to '
                    're-triggered this task.'.format(error['reason'])
                )
            return None
        return {
            'datasetId': dataset_id,
            'tableId': table_id
        }

    def __get_job(self, project_id, job_id):
        job = self.BQ.get_job(
            project_id=project_id,
            job_id=job_id
        )
        logging.info("Job: " + json.dumps(job))
        return job

    @staticmethod
    def __has_errors(job):
        if 'errors' in job['status']:
            logging.error(
                'Copy job finished with errors: %s',
                ', '.join(
                    [x['reason'] + ':' + x.get('message', 'No message provided')
                     for x in job['status']['errors']]
                )
            )
            return True
        return False

    @staticmethod
    def __should_retry(error):
        reason = error['reason']
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
