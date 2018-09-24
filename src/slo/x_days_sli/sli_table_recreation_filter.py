import datetime
import logging

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.slo.x_days_sli.sli_view_querier import SLIViewQuerier


class SLITableRecreationFilter(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def is_recreated(self, sli_table_entry):
        table_reference = SLIViewQuerier.sli_entry_to_table_reference(sli_table_entry)
        table = self.big_query.get_table(
            project_id=table_reference.project_id,
            dataset_id=table_reference.dataset_id,
            table_id=table_reference.table_id)
        table_metadata = BigQueryTableMetadata(table)

        is_table_recreated = table_metadata.get_creation_time() > datetime.datetime.utcfromtimestamp(float(sli_table_entry["creationTime"]) / 1000)

        if is_table_recreated:
            logging.info("Table is recreated till last census snapshot")
            return True
        else:
            logging.info("Table is not recreated till last census snapshot")
            return False
