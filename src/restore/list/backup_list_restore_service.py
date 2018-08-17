from google.appengine.api.datastore_errors import BadRequestError, Error
from google.appengine.ext import ndb

from src.commons.collections import paginated
from src.commons.decorators.log_time import log_time
from src.commons.decorators.retry import retry
from src.commons.exceptions import ParameterValidationException
from src.backup.datastore.Table import Table
from src.restore.async_batch_restore_service import AsyncBatchRestoreService
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.datastore.restore_item import RestoreItem
from src.restore.restoration_table_reference import RestoreTableReference
from src.restore.restore_workspace_creator import RestoreWorkspaceCreator


class BackupItem(object):
    def __init__(self, backup_key, output_parameters=None):
        self.backup_key = backup_key
        self.output_parameters = output_parameters

    def __eq__(self, o):
        return type(o) is BackupItem and self.backup_key == o.backup_key

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.backup_key.__hash__()

    def __str__(self):
        return str(self.backup_key)


class BackupListRestoreRequest(object):
    def __init__(self, backup_items, target_dataset_id=None):
        self.backup_items = backup_items
        self.target_dataset_id = target_dataset_id

    def __eq__(self, o):
        return type(o) is BackupListRestoreRequest \
               and self.backup_items == o.backup_items \
               and self.target_dataset_id == o.target_dataset_id

    def __ne__(self, other):
        return not (self == other)


class BackupListRestoreService(object):
    def restore(self, restoration_job_id, backup_list_restore_request):
        RestorationJob.create(restoration_job_id)
        restore_items = self \
            .__generate_restore_items(backup_list_restore_request)

        AsyncBatchRestoreService().restore(restoration_job_id, [restore_items])

    @log_time
    def __generate_restore_items(self, backup_list_restore_request):
        restore_items = []
        ctx = ndb.get_context()

        for backup_items_sublist \
                in paginated(1000, backup_list_restore_request.backup_items):
            backup_entities = self. \
                __get_backup_entities(backup_items_sublist)

            for backup_item, backup_entity in \
                    zip(backup_items_sublist, backup_entities):
                source_table_entity = self.__get_source_table_entity(
                    backup_entity)

                source_table_reference = RestoreTableReference \
                    .backup_table_reference(source_table_entity, backup_entity)

                target_dataset_id = backup_list_restore_request.target_dataset_id
                if target_dataset_id is None:
                    target_dataset_id = RestoreWorkspaceCreator.create_default_target_dataset_id(
                        source_table_entity.project_id,
                        source_table_entity.dataset_id)

                target_table_reference = \
                    RestoreTableReference.target_table_reference(
                        source_table_entity,
                        target_dataset_id
                    )

                restore_item = RestoreItem.create(source_table_reference,
                                                  target_table_reference,
                                                  backup_item.output_parameters)
                restore_items.append(restore_item)

            ctx.clear_cache()
        return restore_items

    @staticmethod
    @retry(Error, tries=3, delay=1, backoff=2)
    def __get_backup_entities(backup_items):
        try:
            backup_keys = [i.backup_key for i in backup_items]
            for key, entity in \
                    zip(backup_keys, ndb.get_multi(backup_keys,
                                                   use_cache=False,
                                                   use_memcache=False)):
                if not entity:
                    error_message = "Backup entity (key={}) doesn't exist " \
                                    "in datastore.".format(key)
                    raise ParameterValidationException(error_message)
                yield entity
        except BadRequestError, e:
            error_message = "Couldn't obtain backup entity in datastore. " \
                            "Error: \n{}".format(e.message)
            raise ParameterValidationException(error_message)

    @staticmethod
    @retry(Error, tries=3, delay=1, backoff=2)
    def __get_source_table_entity(backup_entity):
        source_table_entity = Table.get_table_from_backup(backup_entity)
        if not source_table_entity:
            error_message = "Backup ancestor doesn't exists: '{}:{}'. " \
                .format(backup_entity.dataset_id,
                        backup_entity.table_id)
            raise ParameterValidationException(error_message)
        return source_table_entity
