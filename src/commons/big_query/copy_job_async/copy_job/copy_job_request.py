from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.big_query.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest


class CopyJobRequest(object):
    def __init__(self, task_name_suffix, copy_job_type_id,
                 source_big_query_table, target_big_query_table,
                 create_disposition, write_disposition,
                 retry_count=0, post_copy_action_request=None,
                 ):
        self.__task_name_suffix = task_name_suffix
        self.__copy_job_type_id = copy_job_type_id
        self.__source_big_query_table = source_big_query_table
        self.__target_big_query_table = target_big_query_table
        self.__retry_count = retry_count
        self.__post_copy_action_request = post_copy_action_request
        self.__create_disposition = create_disposition
        self.__write_disposition = write_disposition

    @property
    def task_name_suffix(self):
        return self.__task_name_suffix

    @property
    def copy_job_type_id(self):
        return self.__copy_job_type_id

    @property
    def retry_count(self):
        return self.__retry_count

    @property
    def source_big_query_table(self):
        return self.__source_big_query_table

    @property
    def target_big_query_table(self):
        return self.__target_big_query_table

    @property
    def create_disposition(self):
        return self.__create_disposition

    @property
    def write_disposition(self):
        return self.__write_disposition

    @property
    def post_copy_action_request(self):
        return self.__post_copy_action_request

    def __str__(self):
        return 'task_name_suffix: {}, copyJobTypeId: {}, sourceTable: {}, ' \
               'targetTable: {}, retryCount: {}, postCopyActionRequest: {}, ' \
               'create_disposition: {}, write_disposition: {}'.format(
                self.__task_name_suffix, self.__copy_job_type_id,
                self.__source_big_query_table, self.__target_big_query_table,
                self.__retry_count, self.__post_copy_action_request,
                self.__create_disposition, self.__write_disposition)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return type(o) is CopyJobRequest \
               and self.__task_name_suffix == o.__task_name_suffix \
               and self.__copy_job_type_id == o.__copy_job_type_id \
               and self.__source_big_query_table == o.__source_big_query_table \
               and self.__target_big_query_table == o.__target_big_query_table \
               and self.__retry_count == o.__retry_count \
               and self.__post_copy_action_request == o.__post_copy_action_request \
               and self.__create_disposition == o.__create_disposition \
               and self.__write_disposition == o.__write_disposition

    def __ne__(self, other):
        return not (self == other)

    def to_json(self):
        return dict(task_name_suffix=self.task_name_suffix,
                    copy_job_type_id=self.copy_job_type_id,
                    source_big_query_table=self.__source_big_query_table,
                    target_big_query_table=self.__target_big_query_table,
                    retry_count=self.__retry_count,
                    post_copy_action_request=self.__post_copy_action_request,
                    create_disposition=self.__create_disposition,
                    write_disposition=self.__write_disposition)

    @classmethod
    def from_json(cls, json):
        source_big_query_table = BigQueryTable.from_json(json["source_big_query_table"])
        target_big_query_table = BigQueryTable.from_json(json["target_big_query_table"])
        post_copy_action_request = PostCopyActionRequest.from_json(json["post_copy_action_request"])

        return CopyJobRequest(
            task_name_suffix=json["task_name_suffix"],
            copy_job_type_id=json["copy_job_type_id"],
            source_big_query_table=source_big_query_table,
            target_big_query_table=target_big_query_table,
            create_disposition=json["create_disposition"],
            write_disposition=json["write_disposition"],
            retry_count=json["retry_count"],
            post_copy_action_request=post_copy_action_request)
