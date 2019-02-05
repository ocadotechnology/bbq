class ResultCheckRequest(object):
    def __init__(self, task_name_suffix, copy_job_type_id, job_reference,
                 retry_count=0, post_copy_action_request=None):
        self.__task_name_suffix = task_name_suffix
        self.__copy_job_type_id = copy_job_type_id
        self.__job_reference = job_reference
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
    def retry_count(self):
        return self.__retry_count

    @property
    def post_copy_action_request(self):
        return self.__post_copy_action_request

    def __str__(self):
        return 'task_name_suffix: {}, copy_job_type_id: {}, jobReference: {},' \
               ' retryCount: {}, postCopyActionRequest: {}'.format(
                self.__task_name_suffix, self.__copy_job_type_id,
                self.__job_reference, self.__retry_count,
                self.__post_copy_action_request)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) is ResultCheckRequest \
               and self.__task_name_suffix == other.__task_name_suffix \
               and self.__copy_job_type_id == other.__copy_job_type_id \
               and self.__job_reference == other.__job_reference \
               and self.__retry_count == other.__retry_count \
               and self.__post_copy_action_request == other.__post_copy_action_request

    def __ne__(self, other):
        return not (self == other)

    def to_json(self):
        return dict(task_name_suffix=self.__task_name_suffix,
                    copy_job_type_id=self.__copy_job_type_id,
                    job_reference=self.__job_reference,
                    retry_count=self.__retry_count,
                    post_copy_action_request=self.__post_copy_action_request)

    @classmethod
    def from_json(cls, json):
        from src.commons.big_query.big_query_job_reference import \
            BigQueryJobReference
        job_reference = BigQueryJobReference.from_json(json["job_reference"])
        from src.commons.big_query.copy_job_async.post_copy_action_request import \
            PostCopyActionRequest

        post_copy_action_request = PostCopyActionRequest.from_json(json["post_copy_action_request"])

        return ResultCheckRequest(
            task_name_suffix=json["task_name_suffix"],
            copy_job_type_id=json["copy_job_type_id"],
            job_reference=job_reference,
            retry_count=json["retry_count"],
            post_copy_action_request=post_copy_action_request)
