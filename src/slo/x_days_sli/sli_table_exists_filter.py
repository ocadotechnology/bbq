import logging

from apiclient.errors import HttpError

from src.commons.big_query.big_query import BigQuery
from src.commons.table_reference import TableReference


class SLITableExistsFilter(object):

    def __init__(self):
        self.big_query = BigQuery()

    def exists(self, table):
        try:
            table_dict = self.big_query.get_table(project_id=table['projectId'],
                                                  dataset_id=table['datasetId'],
                                                  table_id=table['tableId'])
            if table_dict:
                partition_id = table['partitionId']
                if partition_id:
                    return self.__is_partition_exists(
                        project_id=table['projectId'],
                        dataset_id=table['datasetId'],
                        table_id=table['tableId'],
                        partition_id=partition_id)
                return True
        except HttpError as ex:
            if ex.resp.status == 403:
                logging.warning(
                    "Application has no access to '%s'",
                    TableReference(table['projectId'], table['datasetId'],
                                   table['tableId'])
                )
                return False
        logging.info("Table doesn't exist anymore: %s", table)
        return False

    def __is_partition_exists(self, project_id, dataset_id, table_id,
        partition_id):
        partitions = self.big_query.list_table_partitions(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id)

        return partition_id in [partition['partitionId']
                                for partition in partitions]
