import logging

from apiclient.errors import HttpError

from src.commons.big_query.big_query import BigQuery
from src.commons.table_reference import TableReference


class SLIViewQuerier(object):
    def __init__(self):
        self.bigQuery = BigQuery()

    def query(self, x_days):
        query_results = self.bigQuery.execute_query("""SELECT * FROM [project-bbq:SLO_views_legacy.SLI_3_days]""")

        filtered_results = [table for table in query_results if self.table_exists(table)]
        return filtered_results

    def table_exists(self, result):
        project_id = result['f'][0]['v']
        dataset_id = result['f'][1]['v']
        table_id = result['f'][2]['v']

        try:
            table = self.bigQuery.get_table(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_id=table_id)
            if table:
                partition_id = result['f'][3]['v']

                if partition_id:
                    partitions = self.bigQuery.list_table_partitions(
                        project_id=project_id, dataset_id=dataset_id, table_id=table_id)

                    if partition_id in [partition['partitionId'] for partition in partitions]:
                        return True
                    else:
                        return False

                return True
        except HttpError as ex:
            if ex.resp.status == 403:
                logging.warning(
                    "Application has no access to '%s'",
                    TableReference(project_id, dataset_id, table_id)
                )
                return False
        return False






