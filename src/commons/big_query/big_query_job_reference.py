from src.commons.big_query.copy_job_async.result_check.result_check_request import \
    ResultCheckRequest
from src.commons.big_query.copy_job_async.task_creator import TaskCreator


class BigQueryJobReference(object):
    def __init__(self, project_id, job_id, location):
        self.project_id = project_id
        self.job_id = job_id
        self.location = location

    def __str__(self):
        return "BigQueryJobReference(projectId:{}, job_id:{}, location: {})" \
            .format(self.project_id, self.job_id, self.location)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return type(other) is BigQueryJobReference \
               and self.project_id == other.project_id \
               and self.job_id == other.job_id \
               and self.location == other.location

    def __ne__(self, other):
        return not (self == other)

    def create_post_copy_action(self, copy_job_request):
        TaskCreator.create_copy_job_result_check(
            ResultCheckRequest(
                task_name_suffix=copy_job_request.task_name_suffix,
                copy_job_type_id=copy_job_request.copy_job_type_id,
                job_reference=self,
                retry_count=copy_job_request.retry_count,
                post_copy_action_request=copy_job_request.post_copy_action_request
            )
        )

    def to_json(self):
        return dict(project_id=self.project_id,
                    job_id=self.job_id,
                    location=self.location)

    @classmethod
    def from_json(cls, json):
        return BigQueryJobReference(project_id=json["project_id"],
                                    job_id=json["job_id"],
                                    location=json["location"])
