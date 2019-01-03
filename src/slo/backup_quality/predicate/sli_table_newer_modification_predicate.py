import datetime
import logging

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.slo.backup_quality.quality_query_specification import \
    QualityQuerySpecification


class SLITableNewerModificationPredicate(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def is_modified_since_last_census_snapshot(self, sli_table_entry):
        table_reference = QualityQuerySpecification.to_table_reference(sli_table_entry)
        table = self.big_query.get_table(
            project_id=table_reference.get_project_id(),
            dataset_id=table_reference.get_dataset_id(),
            table_id=table_reference.get_table_id_with_partition_id())
        table_metadata = BigQueryTableMetadata(table)

        is_table_modified = table_metadata.get_last_modified_datetime() > datetime.datetime.utcfromtimestamp(sli_table_entry["lastModifiedTime"])

        if is_table_modified:
            logging.info("Table was modified till last census snapshot")
        else:
            logging.info("Table wasn't modified till last census snapshot")

        return is_table_modified
