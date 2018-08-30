import logging


class SLITableExistsFilter(object):

    def __init__(self, big_query):
        self.big_query = big_query

    def exists(self, table_reference):
        table = self.big_query.get_table(
            project_id=table_reference.project_id,
            dataset_id=table_reference.dataset_id,
            table_id=table_reference.table_id)

        if not table:
            logging.info("Table doesn't exist anymore: %s", table_reference)
            return False

        if not table_reference.is_partition():
            return True

        if self.__is_partition_exists(table_reference):
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
