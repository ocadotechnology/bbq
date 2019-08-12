from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.slo.backup_creation_latency.latency_query_specification import \
    LatencyQuerySpecification


class SLITableEmptinessPredicate(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def is_empty(self, sli_table_entry):
        table_reference = LatencyQuerySpecification.to_table_reference(sli_table_entry)
        table = self.big_query.get_table(
            project_id=table_reference.project_id,
            dataset_id=table_reference.dataset_id,
            table_id=table_reference.table_id)
        return BigQueryTableMetadata(table).is_empty()
