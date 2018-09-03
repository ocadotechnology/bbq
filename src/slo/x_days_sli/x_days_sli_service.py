import logging

from src.commons.table_reference import TableReference
from src.commons.big_query.big_query import BigQuery
from src.slo.x_days_sli.sli_table_exists_filter import SLITableExistsFilter
from src.slo.x_days_sli.sli_results_streamer import SLIResultsStreamer
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class XDaysSLIService(object):

    def __init__(self, x_days):
        self.x_days = x_days
        big_query = BigQuery()
        self.querier = SLIViewQuerier(big_query)
        self.streamer = SLIResultsStreamer()
        self.filter = SLITableExistsFilter(big_query)

    def recalculate_sli(self):
        logging.info("Recalculating %s days SLI has been started.", self.x_days)

        all_tables = self.querier.query(self.x_days)
        filtered_tables = [table for table in all_tables
                           if self.__exists(table)]

        logging.info("%s days SLI tables filtered from %s to %s", self.x_days,
                     len(all_tables), len(filtered_tables))
        self.streamer.stream(filtered_tables)

    def __exists(self, table):
        return self.filter.exists(self.__create_table_reference(table))

    @staticmethod
    def __create_table_reference(table):
        return TableReference(project_id=table['projectId'],
                              dataset_id=table['datasetId'],
                              table_id=table['tableId'],
                              partition_id=table['partitionId'])
