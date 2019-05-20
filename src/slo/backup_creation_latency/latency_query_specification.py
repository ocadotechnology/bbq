from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class LatencyQuerySpecification(object):

    def __init__(self, x_days):
        self.x_days = x_days

    def query_string(self):
        projects_to_skip = tuple(configuration.projects_to_skip)
        return "SELECT * FROM " \
               "[{}:SLI_backup_creation_latency_views.SLI_{}_days] " \
               "WHERE projectId NOT IN {}"\
            .format(configuration.metadata_storage_project_id,
                    self.x_days, projects_to_skip)

    def format_query_results(self, results, snapshot_time):
        formatted_results = [{"snapshotTime": snapshot_time,
                              "projectId": result['f'][0]['v'],
                              "datasetId": result['f'][1]['v'],
                              "tableId": result['f'][2]['v'],
                              "partitionId": result['f'][3]['v'],
                              "creationTime": float(result['f'][4]['v']),
                              "lastModifiedTime": float(result['f'][5]['v']),
                              "backupCreated": float(result['f'][6]['v']),
                              "backupLastModified": float(result['f'][7]['v']),
                              "xDays": self.x_days} for result in results]
        return formatted_results

    @staticmethod
    def to_table_reference(table):
        return TableReference(project_id=table['projectId'],
                              dataset_id=table['datasetId'],
                              table_id=table['tableId'],
                              partition_id=table['partitionId'])
