import logging
import time

from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class SLIViewQuerier(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def query(self, x_days):
        self.snapshotTime = time.time()
        query = self.__generate_x_days_sli_query(x_days)
        logging.info("Executing query: %s", query)
        query_results = self.big_query.execute_query(query)

        return self.__format_query_results(query_results, x_days)

    @staticmethod
    def __generate_x_days_sli_query(x_days):
        return \
            "SELECT * FROM [{}:SLI_backup_creation_latency_views.SLI_{}_days]"\
            .format(configuration.backup_project_id, x_days)

    def __format_query_results(self, results, x_days):
        formatted_results = [{"snapshotTime": self.snapshotTime,
                              "projectId": result['f'][0]['v'],
                              "datasetId": result['f'][1]['v'],
                              "tableId": result['f'][2]['v'],
                              "partitionId": result['f'][3]['v'],
                              "creationTime": float(result['f'][4]['v']),
                              "lastModifiedTime": float(result['f'][5]['v']),
                              "backupCreated": float(result['f'][6]['v']),
                              "backupLastModified": float(result['f'][7]['v']),
                              "xDays": x_days} for result in results]

        formatted_results.append(self.__create_snapshot_marker_row(x_days))
        return formatted_results

    def __create_snapshot_marker_row(self, x_days):
        return {"snapshotTime": self.snapshotTime,
                "projectId": 'SNAPSHOT_MARKER',
                "datasetId": 'SNAPSHOT_MARKER',
                "tableId": 'SNAPSHOT_MARKER',
                "partitionId": 'SNAPSHOT_MARKER',
                "creationTime": float(0),
                "lastModifiedTime": float(0),
                "backupCreated": float(0),
                "backupLastModified": float(0),
                "xDays": x_days}

    @staticmethod
    def sli_entry_to_table_reference(table):
        return TableReference(project_id=table['projectId'],
                              dataset_id=table['datasetId'],
                              table_id=table['tableId'],
                              partition_id=table['partitionId'])