import logging

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata

class SLITableExistsPredicate(object):

    def __init__(self, big_query, query_specification):
        self.big_query = big_query
        self.query_specification = query_specification

    def exists(self, sli_table):
        table_reference = self.query_specification.to_table_reference(sli_table)
        table = self.big_query.get_table(
            project_id=table_reference.project_id,
            dataset_id=table_reference.dataset_id,
            table_id=table_reference.table_id)

        table_metadata = BigQueryTableMetadata(table)

        if not table_metadata.table_exists():
            logging.info("Table doesn't exist anymore: %s", table_reference)
            return False

        if not table_metadata.is_schema_defined():
            logging.info("Table doesn't have schema. Ignoring table: %s",
                         table_reference)
            return False

        if not table_reference.is_partition() or \
           not table_metadata.has_time_partitioning():
            logging.info("Non-partitioned table exist: %s", table_reference)
            return True

        if self.__is_partition_exists(table_reference):
            logging.info("Table partition exist: %s", table_reference)
            return True

        logging.info("Partition doesn't exist anymore: %s", table_reference)
        return False

    def __is_partition_exists(self, table_reference):
        partitions = self.big_query.list_table_partitions(
            project_id=table_reference.project_id,
            dataset_id=table_reference.dataset_id,
            table_id=table_reference.table_id)

        return table_reference.partition_id in [partition['partitionId']
                                                for partition in partitions]
