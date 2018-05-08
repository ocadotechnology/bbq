import datetime
import logging
from types import NoneType

from src.error_reporting import ErrorReporting
from src.table_reference import TableReference


class BigQueryTableMetadata(object):
    def __init__(self, big_query_table):
        assert isinstance(big_query_table, (dict, NoneType))
        self.table_metadata = big_query_table

    def table_exists(self):
        if self.table_metadata is None:
            return False
        return True

    def is_external_or_view_type(self):
        if 'type' in self.table_metadata:
            table_type = self.table_metadata['type']
            if table_type == 'VIEW':
                logging.info('This is a VIEW, ignoring')
                return True
            elif table_type == 'EXTERNAL':
                logging.info('This is a EXTERNAL, ignoring')
                return True
        return False

    def is_empty(self):
        return self.table_metadata['numRows'] == '0'

    def get_creation_time(self):
        return self.__convert_to_datetime(
            self.table_metadata['creationTime'])

    def get_last_modified_datetime(self):
        return self.__convert_to_datetime(
            self.table_metadata['lastModifiedTime'])

    def has_partition_expiration(self):
        return self.has_time_partitioning() \
               and 'expirationMs' in self.table_metadata['timePartitioning']

    @staticmethod
    def __convert_to_datetime(timestamp):
        return datetime.datetime.utcfromtimestamp(float(timestamp) / 1000)

    def table_size_in_bytes(self):
        return int(self.table_metadata['numBytes'])

    def table_rows_count(self):
        return int(self.table_metadata['numRows'])

    def get_location(self):
        return self.table_metadata.get('location', 'US')

    def is_localized_in_EU(self):
        return self.get_location() == 'EU'

    def is_daily_partitioned(self):
        if self.table_metadata and 'timePartitioning' in self.table_metadata:
            if self.is_partition():
                return False
            time_partitioning = self.table_metadata['timePartitioning']
            if 'type' in time_partitioning:
                type_of_partitioning = time_partitioning['type']
                if type_of_partitioning == 'DAY':
                    return True
            ErrorReporting().report(("Table metadata has different structure "
                                     "than expected: {0}")
                                    .format(self.table_metadata))

        return False

    def is_partition(self):
        table_id = self.table_metadata['tableReference']['tableId']
        return '$' in table_id

    def has_time_partitioning(self):
        return 'timePartitioning' in self.table_metadata

    def get_partition_id(self):
        assert self.is_partition() == True
        table_id = self.table_metadata['tableReference']['tableId']
        return table_id.split('$')[1]

    def get_table_id(self):
        table_id = self.table_metadata['tableReference']['tableId']
        return table_id.split('$')[0]

    def table_reference(self):
        table_reference = self.table_metadata['tableReference']
        if self.is_partition():
            return TableReference(table_reference['projectId'],
                                  table_reference['datasetId'],
                                  self.get_table_id(), self.get_partition_id())
        else:
            return TableReference(table_reference['projectId'],
                                  table_reference['datasetId'],
                                  table_reference['tableId'])
