from datetime import datetime

from src.big_query.big_query import BigQuery
from src.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.big_query.copy_job_service import CopyJobService


class DatasetNotFoundException(Exception):
    pass


class RestoreTableException(Exception):
    pass


class BigQueryTableRestorer(object):

    def __init__(self):
        self.BQ = BigQuery()

    def restore_table(self, backup_table_reference, restore_table_reference):
        self.__create_dataset_if_doesnt_exists(backup_table_reference,
                                               restore_table_reference)

        if restore_table_reference.is_partition():
            self.BQ.create_empty_partitioned_table(restore_table_reference)

        result = CopyJobService(datetime.utcnow()).copy_table(
            backup_table_reference.create_big_query_table(),
            restore_table_reference.create_big_query_table(),
            write_disposition='WRITE_TRUNCATE')
        if not result:
            return BigQueryTableMetadata(result)
        return self.__get_restored_table_metadata(restore_table_reference)

    def __create_dataset_if_doesnt_exists(self, backup_table_reference,
                                          restore_table_reference):
        location = self.BQ.get_dataset_location(
            backup_table_reference.project_id,
            backup_table_reference.dataset_id)
        table_expiration = (3600000 * 24 * 7)  # 7 days
        self.BQ.create_dataset(
            restore_table_reference.project_id,
            restore_table_reference.dataset_id,
            location,
            table_expiration
        )

    def __get_restored_table_metadata(self, restore_table_reference):
        restored_table_metadata = self.BQ.get_table_by_reference(
            restore_table_reference)

        if not restored_table_metadata.table_exists():
            raise RestoreTableException('Restored table was not found: {}'
                                        .format(restore_table_reference))

        return restored_table_metadata
