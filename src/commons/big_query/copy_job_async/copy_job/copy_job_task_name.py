import logging

from datetime import datetime


class CopyJobTaskName(object):

    def __init__(self, copy_job_request):
        self.__copy_job_request = copy_job_request

    # Regarding the API restriction - task name needs to match
    # following expression: "^[a-zA-Z0-9_-]{1,500}$".
    #
    # This method return None in case of name that exceeds 500 characters,
    # what protect us from failures where source table name is very long,
    # but still valid.
    #
    # The disadvantage is - if task_name is None, that means
    # it's not unique and the same backup may be created more than once.
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
        return task_name if len(task_name) <= 500 else None
