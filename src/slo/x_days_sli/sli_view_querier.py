import logging
from datetime import datetime

from apiclient.errors import HttpError

from src.commons.big_query.big_query import BigQuery
from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class SLIViewQuerier(object):
    def __init__(self):
        self.bigQuery = BigQuery()

    def query(self, x_days):

        query_results = self.bigQuery.execute_query(
            self.__generate_x_days_sli_query(x_days))

        filtered_results = [table for table in query_results if self.__table_exists(table)]

        return self.__format_query_results(filtered_results,datetime.now())

    def __table_exists(self, result_row):
        project_id, dataset_id, table_id = self.__extract_table_name(result_row)

        try:
            table = self.bigQuery.get_table(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_id=table_id)
            if table:
                partition_id = self.__extract_partition_id(result_row)
                if partition_id:
                    return self.__is_partition_exists(project_id=project_id,
                                                      dataset_id=dataset_id,
                                                      table_id=table_id,
                                                      partition_id=partition_id)
                return True
        except HttpError as ex:
            if ex.resp.status == 403:
                logging.warning(
                    "Application has no access to '%s'",
                    TableReference(project_id, dataset_id, table_id)
                )
                return False
        return False

    @staticmethod
    def __generate_x_days_sli_query(x_days):
        return "SELECT * FROM [{}:SLO_views_legacy.SLI_{}_days]".format(
            configuration.backup_project_id, x_days)

    @staticmethod
    def __extract_table_name(result):
        project_id = result['f'][0]['v']
        dataset_id = result['f'][1]['v']
        table_id = result['f'][2]['v']
        return project_id, dataset_id, table_id

    @staticmethod
    def __extract_partition_id(result):
        return result['f'][3]['v']

    def __is_partition_exists(self,project_id, dataset_id, table_id,partition_id):
        partitions = self.bigQuery.list_table_partitions(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id)

        if partition_id in [partition['partitionId'] for partition in partitions]:
            return True
        else:
            return False

    def __format_query_results(self, results, snapshot_time):
        return [{
            "snapshotTime": snapshot_time,
            "projectId": result['f'][0]['v'],
            "datasetId":result['f'][1]['v'],
            "tableId":result['f'][2]['v'],
            "partitionId":result['f'][3]['v'],
            "creationTime":result['f'][4]['v'],
            "lastModifiedTime":result['f'][5]['v'],
            "backup_created":result['f'][6]['v'],
            "backup_last_modified":result['f'][7]['v']
            } for result in results
        ]


