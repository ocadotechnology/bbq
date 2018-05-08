import logging

from datetime import datetime


class CopyJobTaskName(object):

    def __init__(self, copy_job_request):
        self.__copy_job_request = copy_job_request

    def create(self):
        logging.info("INFO:  %s", self.__copy_job_request)
        return '_'.join([
            datetime.utcnow().strftime("%Y-%m-%d"),
            str(self.__copy_job_request.source_big_query_table),
            str(self.__copy_job_request.retry_count),
            str(self.__copy_job_request.task_name_suffix)
        ])\
            .replace('$', '_')\
            .replace('.', '_')\
            .replace(':', '_')
# The task name needs to match following expression: "^[a-zA-Z0-9_-]{1,500}$"
