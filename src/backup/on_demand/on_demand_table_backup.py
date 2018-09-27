from src.backup.backup_process import BackupProcess
from src.backup.on_demand.on_demand_should_backup_predicate import \
  OnDemandShouldBackupPredicate
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.exceptions import ParameterValidationException


class PartitionedTableWithoutPartition(Exception):
    pass


class OnDemandTableBackup(object):

    @staticmethod
    def start(table_reference):
        big_query_table_metadata = BigQueryTableMetadata.get_table_by_reference(table_reference)

        if big_query_table_metadata.is_daily_partitioned() and not big_query_table_metadata.is_partition():
            raise ParameterValidationException("Partition id is required for partitioned table in on-demand mode")

        BackupProcess(table_reference=table_reference,
                      big_query=BigQuery(),
                      big_query_table_metadata=big_query_table_metadata,
                      should_backup_predicate=OnDemandShouldBackupPredicate()).start()
