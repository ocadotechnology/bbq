import json
import logging

from src.commons.big_query.copy_job_async.task_creator import TaskCreator


class BigQueryJobError(object):
    def __init__(self, bq_error, source_bq_table, target_bq_table):
        self.bq_error = bq_error
        self.source_bq_table = source_bq_table
        self.target_bq_table = target_bq_table
        self.full_reason = bq_error._get_reason()
        self.short_reason = self.__get_short_reason()
        self.location = self.__get_location()

    def __str__(self):
        return "{} while creating Copy Job from {} to {}" \
            .format(self.short_reason, self.source_bq_table, self.target_bq_table)

    def create_post_copy_action(self, copy_job_request):
        logging.error(self)
        if copy_job_request.post_copy_action_request is not None:
            TaskCreator.create_post_copy_action(
                copy_job_type_id=copy_job_request.copy_job_type_id,
                post_copy_action_request=copy_job_request.post_copy_action_request,
                job_json=self.__create_post_copy_job_json()
            )

    def should_be_retried(self):
        if self.__is_access_denied() or self.__is_404():
            return False
        elif self.__is_409():
            return True
        else:
            raise self.bq_error

    def is_deadline_exceeded(self):
        return self.bq_error.resp.status == 500 and \
               'Deadline exceeded' in self.full_reason

    def __is_access_denied(self):
        return self.bq_error.resp.status == 403 and \
               'Access Denied' in self.full_reason

    def __is_404(self):
        return self.bq_error.resp.status == 404

    def __is_409(self):
        return self.bq_error.resp.status == 409

    def __get_short_reason(self):
        if self.__is_access_denied():
            return 'Access Denied'
        elif self.__is_404():
            return '404'
        elif self.__is_409():
            return '409'
        else:
            return 'Unknown reason'

    def __get_location(self):
        try:
            data = json.loads(self.bq_error.content)
            return data['error']['errors'][0]['location']
        except (ValueError, KeyError):
            return None

    def __create_post_copy_job_json(self):
        return {
            "status": {
                "state": "DONE",
                "errors": [
                    {
                        "reason": 'Invalid',
                        "message": self.__str__()
                    }
                ]
            },
            "configuration": {
                "copy": {
                    "sourceTable": {
                        "projectId": self.source_bq_table.get_project_id(),
                        "tableId": self.source_bq_table.get_table_id(),
                        "datasetId": self.source_bq_table.get_dataset_id()
                    },
                    "destinationTable": {
                        "projectId": self.target_bq_table.get_project_id(),
                        "tableId": self.target_bq_table.get_table_id(),
                        "datasetId": self.target_bq_table.get_dataset_id()
                    }
                }
            }
        }
