import logging

from google.appengine.ext import ndb

from src.commons.decorators.log_time import log_time
from src.backup.copy_job_async.copy_job_service_async import CopyJobServiceAsync
from src.backup.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest
from src.big_query.big_query import BigQuery
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.restore_workspace_creator import RestoreWorkspaceCreator


class AsyncBatchRestoreService(object):

    def __init__(self):
        self.BQ = BigQuery()
        self.restore_workspace_creator = RestoreWorkspaceCreator(self.BQ)

    def restore(self, restoration_job_id, restore_items_list_generator):

        for restore_item_list in restore_items_list_generator:
            self.__save_restore_items_in_ds(restoration_job_id,
                                            restore_item_list)
            self.__run_copy_job_for_each(restore_item_list, restoration_job_id)

    @log_time
    def __run_copy_job_for_each(self, restore_items, restoration_job_id):
        logging.info("Scheduling %s", len(restore_items))
        for restore_item in restore_items:

            source_table_reference = restore_item.source_table_reference
            target_table_reference = restore_item.target_table_reference


            try:
                self.restore_workspace_creator.create_workspace(
                    source_table_reference,
                    target_table_reference)
                CopyJobServiceAsync(
                    copy_job_type_id='restore',
                    task_name_suffix=restoration_job_id
                ).with_post_action(PostCopyActionRequest(
                    url='/callback/restore-finished/',
                    data={'restoreItemKey': restore_item.key.urlsafe()})
                ).copy_table(
                    source_table_reference.create_big_query_table(),
                    target_table_reference.create_big_query_table()
                )
            except Exception as ex:
                logging.error("Error during creating copy job. Marking restore "
                              "item as FAILED, Error message: %s", ex.message)
                restore_item.update_with_failed(restore_item.key, ex.message)

    @staticmethod
    @log_time
    def __save_restore_items_in_ds(restoration_job_id, restore_items):
        logging.info("Datastore items update")
        restoration_job = RestorationJob.get_by_id(restoration_job_id)
        restoration_job.increment_count_by(len(restore_items))

        for restore_item in restore_items:
            restore_item.restoration_job_key = restoration_job.key

        ndb.put_multi(restore_items, use_cache=False)

        ctx = ndb.get_context()
        ctx.clear_cache()
