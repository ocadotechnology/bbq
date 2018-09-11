import logging

import time

from src.commons.config.configuration import configuration


class SLIViewQuerier(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def query(self, x_days):
        query = self.__generate_x_days_sli_query(x_days)
        logging.info("Executing query: %s", query)
        query_results = self.big_query.execute_query(query)

        return self.__format_query_results(query_results, x_days)

    @staticmethod
    def __generate_x_days_sli_query(x_days):
        return \
            "SELECT * FROM [{}:SLI_backup_creation_latency_views.SLI_{}_days]"\
            .format(configuration.backup_project_id, x_days)

    @staticmethod
    def __format_query_results(results, x_days):
        return [{
            "snapshotTime": time.time(),
            "projectId": result['f'][0]['v'],
            "datasetId": result['f'][1]['v'],
            "tableId": result['f'][2]['v'],
            "partitionId": result['f'][3]['v'],
            "creationTime": float(result['f'][4]['v']) / 1000.0,
            "lastModifiedTime": float(result['f'][5]['v']) / 1000.0,
            "backupCreated": float(result['f'][6]['v']) / 1000.0,
            "backupLastModified": float(result['f'][7]['v']) / 1000.0,
            "xDays": x_days
            } for result in results
        ]
