import logging
import re

from datetime import datetime


# The task name needs to match following expression
EXPRESSION = re.compile("^[a-zA-Z0-9_-]{1,500}$")


class CopyJobTaskName(object):

    def __init__(self, copy_job_request):
        self.__copy_job_request = copy_job_request

    def create(self):
        logging.info("INFO:  %s", self.__copy_job_request)
        task_name = '_'.join([
            datetime.utcnow().strftime("%Y-%m-%d"),
            str(self.__copy_job_request.source_big_query_table),
            str(self.__copy_job_request.retry_count),
            str(self.__copy_job_request.task_name_suffix)
        ])\
            .replace('$', '_')\
            .replace('.', '_')\
            .replace(':', '_')
        return task_name if EXPRESSION.match(task_name) else None
