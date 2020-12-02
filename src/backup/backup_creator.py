import logging
import urllib2

from src.backup.backup_id_creator import BackupIdCreator
from src.backup.dataset_id_creator import DatasetIdCreator
from src.commons import request_correlation_id
from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.big_query.copy_job_async.copy_job_service_async \
    import CopyJobServiceAsync
from src.commons.big_query.copy_job_async.post_copy_action_request \
    import \
    PostCopyActionRequest
from src.commons.config.configuration import configuration


class BackupCreator(object):

    def __init__(self, now):
        self.now = now

    def create_backup(self, source_table_entity, bq_table_metadata):
        logging.info(u'Scheduling copy job for backup, request correlation id:'
                     u' %s', request_correlation_id.get())

        target_project_id = configuration.backup_project_id
        target_dataset_id = DatasetIdCreator.create(self.now,
                                                    bq_table_metadata.get_location(),
                                                    source_table_entity.project_id)
        target_table_id = self.__create_table_id(source_table_entity)

        source_table_id_with_partition_id = BigQueryTableMetadata \
            .get_table_id_with_partition_id(source_table_entity.table_id,
                                            source_table_entity.partition_id)

        source_bq_table = BigQueryTable(source_table_entity.project_id,
                                        source_table_entity.dataset_id,
                                        source_table_id_with_partition_id)
        destination_bq_table = BigQueryTable(target_project_id,
                                             target_dataset_id,
                                             target_table_id)

        self.__copy_table_async(source_bq_table, destination_bq_table)

    @staticmethod
    def __copy_table_async(source_bq_table, destination_bq_table):
        CopyJobServiceAsync(
            copy_job_type_id='backups',
            task_name_suffix=request_correlation_id.get()
        ).with_post_action(PostCopyActionRequest(
            url=u'/callback/backup-created/{}/{}/{}'
                .format(source_bq_table.project_id,
                        source_bq_table.dataset_id,
                        urllib2.quote(
                            source_bq_table.table_id.encode('UTF-8'))),
            data={"sourceBqTable": source_bq_table,
                  "targetBqTable": destination_bq_table
                  })
        ).copy_table(source_bq_table, destination_bq_table)

    def __create_table_id(self, source_table_entity):
        return BackupIdCreator.create(
            source_table_entity.project_id,
            source_table_entity.dataset_id,
            source_table_entity.table_id,
            self.now,
            source_table_entity.partition_id
        )
