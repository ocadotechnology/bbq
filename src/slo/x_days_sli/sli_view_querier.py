import logging
from datetime import datetime


from src.commons.config.configuration import configuration


class SLIViewQuerier(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def query(self, x_days):
        query = self.__generate_x_days_sli_query(x_days)
        logging.info("Executing query: %s", query)
        query_results = self.big_query.execute_query(query)

        return self.__format_query_results(query_results, x_days,
                                           datetime.now())

    @staticmethod
    def __generate_x_days_sli_query(x_days):
        return "SELECT * FROM [{}:SLO_views_legacy.SLI_{}_days]".format(
            configuration.backup_project_id, x_days)

    def __format_query_results(self, results, x_days, snapshot_time):
        return [{
            "snapshotTime": snapshot_time,
            "projectId": result['f'][0]['v'],
            "datasetId": result['f'][1]['v'],
            "tableId": result['f'][2]['v'],
            "partitionId": result['f'][3]['v'],
            "creationTime": result['f'][4]['v'],
            "lastModifiedTime": result['f'][5]['v'],
            "backupCreated": result['f'][6]['v'],
            "backupLastModified": result['f'][7]['v'],
            "xDays": x_days
            } for result in results
        ]
