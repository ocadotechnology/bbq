from src.commons.big_query.copy_job_async.copy_job.copy_job_request import CopyJobRequest
from src.commons.big_query.copy_job_async.task_creator import TaskCreator


class CopyJobServiceAsync(object):

    def __init__(self, copy_job_type_id, task_name_suffix):
        self.__copy_job_type_id = copy_job_type_id
        self.__task_name_suffix = task_name_suffix
        self.__post_copy_action_request = None
        self.__create_disposition = "CREATE_IF_NEEDED"
        self.__write_disposition = "WRITE_EMPTY"
        assert self.__copy_job_type_id is not None, "copy_job_type_id needs to be assigned in constructor"
        assert self.__task_name_suffix is not None, "task_name_suffix needs to be assigned in constructor"

    def with_post_action(self, post_copy_action_request):
        self.__post_copy_action_request = post_copy_action_request
        return self

    def with_create_disposition(self, create_disposition):
        self.__create_disposition = create_disposition
        return self

    def with_write_disposition(self, write_disposition):
        self.__write_disposition = write_disposition
        return self

    def copy_table(self, source_big_query_table, target_big_query_table):
        copy_job_request = CopyJobRequest(
            task_name_suffix=self.__task_name_suffix,
            copy_job_type_id=self.__copy_job_type_id,
            source_big_query_table=source_big_query_table,
            target_big_query_table=target_big_query_table,
            retry_count=0,
            post_copy_action_request=self.__post_copy_action_request,
            create_disposition=self.__create_disposition,
            write_disposition=self.__write_disposition
        )
        TaskCreator.create_copy_job(copy_job_request)
