class ResultCheckRequest(object):
    def __init__(self, task_name_suffix, copy_job_type_id, job_reference,
                 create_disposition, write_disposition,
                 retry_count=0, post_copy_action_request=None):
        self.__task_name_suffix = task_name_suffix
        self.__copy_job_type_id = copy_job_type_id
        self.__job_reference = job_reference
        self.__create_disposition = create_disposition
        self.__write_disposition = write_disposition
        self.__retry_count = retry_count
        self.__post_copy_action_request = post_copy_action_request

    @property
    def task_name_suffix(self):
        return self.__task_name_suffix

    @property
    def copy_job_type_id(self):
        return self.__copy_job_type_id

    @property
    def job_reference(self):
        return self.__job_reference

    @property
    def create_disposition(self):
        return self.__create_disposition

    @property
    def write_disposition(self):
        return self.__write_disposition

    @property
    def retry_count(self):
        return self.__retry_count

    @property
    def post_copy_action_request(self):
        return self.__post_copy_action_request

    def __str__(self):
        return 'task_name_suffix: {}, copy_job_type_id: {}, jobReference: {},' \
               ' create_disposition: {}, write_disposition: {},'  \
               ' retryCount: {}, postCopyActionRequest: {}'.format(
                self.__task_name_suffix, self.__copy_job_type_id,
                self.__job_reference, self.__create_disposition,
                self.__write_disposition, self.__retry_count,
                self.__post_copy_action_request)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) is ResultCheckRequest \
               and self.__task_name_suffix == other.__task_name_suffix \
               and self.__copy_job_type_id == other.__copy_job_type_id \
               and self.__job_reference == other.__job_reference \
               and self.__create_disposition == other.__create_disposition \
               and self.__write_disposition == other.__write_disposition \
               and self.__retry_count == other.__retry_count \
               and self.__post_copy_action_request == other.__post_copy_action_request

    def __ne__(self, other):
        return not (self == other)
