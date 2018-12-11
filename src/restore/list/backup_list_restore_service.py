import uuid

from google.appengine.api.datastore_errors import BadRequestError, Error
from google.appengine.ext import ndb

from src.backup.datastore.Table import Table
from src.commons.collections import paginated
from src.commons.decorators.log_time import log_time
from src.commons.decorators.retry import retry
from src.commons.exceptions import ParameterValidationException
from src.commons.table_reference import TableReference
from src.restore.async_batch_restore_service import AsyncBatchRestoreService
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.datastore.restore_item import RestoreItem
from src.restore.restoration_table_reference import RestoreTableReference


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
    def __init__(self, backup_items, target_project_id, target_dataset_id,
                 create_disposition='CREATE_IF_NEEDED',
                 write_disposition='WRITE_EMPTY'):
        self.backup_items = backup_items
        self.target_project_id = target_project_id
        self.target_dataset_id = target_dataset_id
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.restoration_job_id = str(uuid.uuid4())

    def __eq__(self, o):
        return type(o) is BackupListRestoreRequest \
               and self.backup_items == o.backup_items \
               and self.target_dataset_id == o.target_dataset_id \
               and self.target_project_id == o.target_project_id \
               and self.write_disposition == o.write_disposition \
               and self.create_disposition == o.create_disposition \
               and self.restoration_job_id == o.restoration_job_id

    def __ne__(self, other):
        return not (self == other)


class BackupListRestoreService(object):
    def restore(self, restore_request):
        restoration_job_key = RestorationJob.create(
            restoration_job_id=restore_request.restoration_job_id,
            create_disposition=restore_request.create_disposition,
            write_disposition=restore_request.write_disposition
        )
        restore_items = self.__generate_restore_items(restore_request)

        AsyncBatchRestoreService().restore(restoration_job_key, [restore_items])
        return restore_request.restoration_job_id

    @log_time
    def __generate_restore_items(self, restore_request):
        restore_items = []
        ctx = ndb.get_context()

        for backup_items_sublist in paginated(1000, restore_request.backup_items):
            backup_entities = self.__get_backup_entities(backup_items_sublist)

            for backup_item, backup_entity in zip(backup_items_sublist, backup_entities):
                restore_item = self.__create_restore_item(
                    restore_request,
                    backup_entity,
                    backup_item
                )
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

    def __create_restore_item(self, restore_request, backup_entity, backup_item):
        source_entity = self.__get_source_table_entity(backup_entity)

        source_table_reference = RestoreTableReference \
            .backup_table_reference(source_entity, backup_entity)
        target_table_reference = self.__create_target_table_reference(
            restore_request, source_entity)

        return RestoreItem.create(source_table_reference,
                                  target_table_reference,
                                  backup_item.output_parameters)

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

    @staticmethod
    def __create_target_table_reference(restore_request, source_entity):
        target_project_id = restore_request.target_project_id
        target_dataset_id = restore_request.target_dataset_id

        if target_project_id is None:
            target_project_id = source_entity.project_id
        if target_dataset_id is None:
            target_dataset_id = source_entity.dataset_id
        return TableReference(target_project_id,
                              target_dataset_id,
                              source_entity.table_id,
                              source_entity.partition_id)
