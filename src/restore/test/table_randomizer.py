import logging
import random
from datetime import datetime, timedelta

from commons.decorators.retry import retry
from src.big_query.big_query import BigQuery, RandomizationError
from src.table_reference import TableReference


class DoesNotMeetSampleCriteriaException(BaseException):
    pass


class TableRandomizer(object):

    def __init__(self):
        self.big_query = BigQuery()

    @retry((RandomizationError, DoesNotMeetSampleCriteriaException),
           tries=10, delay=0, backoff=0)
    def get_random_table_metadata(self):
        table_reference = self.big_query.fetch_random_table()
        logging.info("Randomly selected table for the restore test: %s",
                     table_reference)

        table_metadata = self.big_query.get_table_by_reference(table_reference)

        if not table_metadata.table_exists():
            raise DoesNotMeetSampleCriteriaException("Table not found")

        if table_metadata.is_external_or_view_type():
            raise DoesNotMeetSampleCriteriaException(
                "Table is a view or external.")

        if self.__has_been_modified_since_midnight(table_metadata):
            raise DoesNotMeetSampleCriteriaException(
                "Table has been modified since midnight")

        if self.__was_modified_yesterday(table_metadata):
            logging.error("Table was modified yesterday. "
                          "It is possible that last backup cycle did not cover"
                          " it. Therefore restoration of this table can fail.")

        if table_metadata.is_daily_partitioned() and not table_metadata.is_empty():
            table_metadata = self.__get_random_partition(table_reference)
            logging.info(
                "Table is partitioned. Partition chosen to be restored: %s",
                table_metadata)

        return table_metadata

    def __get_random_partition(self, table_reference):
        partitions = self.big_query.list_table_partitions(table_reference.project_id,
                                                          table_reference.dataset_id,
                                                          table_reference.table_id)
        random_partition = self.__get_random_item_of_the_list(partitions)

        new_table_reference = TableReference(table_reference.project_id, table_reference.dataset_id, table_reference.table_id, random_partition)

        return self.big_query.get_table_by_reference(new_table_reference)

    def __get_random_item_of_the_list(self, partitions):
        number_of_partitions = len(partitions)
        random_partition = partitions[
            random.randint(0, number_of_partitions - 1)]["partitionId"]
        return random_partition

    @staticmethod
    def __has_been_modified_since_midnight(table_metadata):
        d = datetime.utcnow().date()
        midnight = datetime(d.year, d.month, d.day)
        return table_metadata.get_last_modified_datetime() > midnight

    @staticmethod
    def __was_modified_yesterday(table_metadata):
        d = datetime.utcnow().date()
        last_midnight = datetime(d.year, d.month, d.day)
        midnight_day_before = (last_midnight - timedelta(days=1))
        return table_metadata.get_last_modified_datetime() > midnight_day_before