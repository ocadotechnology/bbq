from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class QualityQuerySpecification(object):

    def query_string(self):
        return \
            "SELECT * FROM [{}:SLI_backup_quality_views.SLI_quality]"\
              .format(configuration.backup_project_id)

    def format_query_results(self, results, snapshot_time):
        formatted_results = [{"snapshotTime": snapshot_time,
                              "projectId": result['f'][0]['v'],
                              "datasetId": result['f'][1]['v'],
                              "tableId": result['f'][2]['v'],
                              "partitionId": result['f'][3]['v'],
                              "backupDatasetId": result['f'][4]['v'],
                              "backupTableId": result['f'][5]['v'],
                              "lastModifiedTime": float(result['f'][6]['v']),
                              "backupLastModifiedTime": float(result['f'][7]['v']) if result['f'][7]['v'] is not None else None,
                              "backupEntityLastModifiedTime": float(result['f'][8]['v']),
                              "numBytes": int(result['f'][9]['v']),
                              "backupNumBytes": int(result['f'][10]['v']) if result['f'][10]['v'] is not None else None,
                              "backupEntityNumBytes": int(result['f'][11]['v']),
                              "numRows": int(result['f'][12]['v']),
                              "backupNumRows": int(result['f'][13]['v']) if result['f'][13]['v'] is not None else None,
                              } for result in results]
        return formatted_results

    @staticmethod
    def to_table_reference(table):
        return TableReference(project_id=table['projectId'],
                              dataset_id=table['datasetId'],
                              table_id=table['tableId'],
                              partition_id=table['partitionId'])
