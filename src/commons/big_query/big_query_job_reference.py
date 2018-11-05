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
