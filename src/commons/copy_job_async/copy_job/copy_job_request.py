class CopyJobRequest(object):
    def __init__(self, task_name_suffix, copy_job_type_id,
                 source_big_query_table, target_big_query_table,
                 retry_count=0, post_copy_action_request=None):
        self.__task_name_suffix = task_name_suffix
        self.__copy_job_type_id = copy_job_type_id
        self.__source_big_query_table = source_big_query_table
        self.__target_big_query_table = target_big_query_table
        self.__retry_count = retry_count
        self.__post_copy_action_request = post_copy_action_request

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
    def post_copy_action_request(self):
        return self.__post_copy_action_request

    def __str__(self):
        return 'task_name_suffix: {}, copyJobTypeId: {}, sourceTable: {}, targetTable: {}, retryCount: {} ' \
               ', postCopyActionRequest: {}'.format(
            self.__task_name_suffix,
            self.__copy_job_type_id,
            self.__source_big_query_table, self.__target_big_query_table,
            self.__retry_count, self.__post_copy_action_request)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return type(o) is CopyJobRequest \
               and self.__task_name_suffix == o.__task_name_suffix \
               and self.__copy_job_type_id == o.__copy_job_type_id \
               and self.__source_big_query_table == o.__source_big_query_table \
               and self.__target_big_query_table == o.__target_big_query_table \
               and self.__retry_count == o.__retry_count \
               and self.__post_copy_action_request == o.__post_copy_action_request

    def __ne__(self, other):
        return not (self == other)
