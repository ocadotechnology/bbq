class ResultCheckRequest(object):
    def __init__(self, task_name_suffix, copy_job_type_id, project_id, job_id,
                 location, retry_count=0, post_copy_action_request=None):
        self.__task_name_suffix = task_name_suffix
        self.__copy_job_type_id = copy_job_type_id
        self.__project_id = project_id
        self.__job_id = job_id
        self.__location = location
        self.__retry_count = retry_count
        self.__post_copy_action_request = post_copy_action_request

    @property
    def task_name_suffix(self):
        return self.__task_name_suffix

    @property
    def copy_job_type_id(self):
        return self.__copy_job_type_id

    @property
    def project_id(self):
        return self.__project_id

    @property
    def job_id(self):
        return self.__job_id

    @property
    def location(self):
        return self.__location

    @property
    def retry_count(self):
        return self.__retry_count

    @property
    def post_copy_action_request(self):
        return self.__post_copy_action_request

    def __str__(self):
        return 'task_name_suffix: {}, copy_job_type_id: {}, projectId: {}, ' \
               'jobId: {}, location: {}, retryCount: {},' \
               ' postCopyActionRequest: {}'.format(
            self.__task_name_suffix, self.__copy_job_type_id,
            self.__project_id, self.__job_id, self.__location,
            self.__retry_count, self.__post_copy_action_request)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return type(o) is ResultCheckRequest \
               and self.__task_name_suffix == o.__task_name_suffix \
               and self.__copy_job_type_id == o.__copy_job_type_id \
               and self.__project_id == o.__project_id \
               and self.__job_id == o.__job_id \
               and self.__location == o.__location \
               and self.__retry_count == o.__retry_count \
               and self.__post_copy_action_request == o.__post_copy_action_request

    def __ne__(self, other):
        return not (self == other)
