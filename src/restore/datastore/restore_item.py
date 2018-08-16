import json
import logging
from datetime import datetime

from google.appengine.ext import ndb

from src.backup.datastore.Table import query_using_cursor
from src.commons.table_reference import TableReference


class TableReferenceEntity(ndb.Model):
    project_id = ndb.StringProperty(indexed=True)
    dataset_id = ndb.StringProperty(indexed=True)
    table_id = ndb.StringProperty(indexed=True)
    partition_id = ndb.StringProperty(indexed=True)

    @classmethod
    def create(cls, table):
        return TableReferenceEntity(
            project_id=table.project_id,
            dataset_id=table.dataset_id,
            table_id=table.table_id,
            partition_id=table.partition_id
        )


class RestoreItem(ndb.Model):
    restoration_job_key = ndb.KeyProperty(indexed=True)
    status = ndb.StringProperty(indexed=True)
    status_message = ndb.StringProperty(indexed=False)
    completed = ndb.DateTimeProperty(indexed=True)
    source_table = ndb.StructuredProperty(TableReferenceEntity)
    target_table = ndb.StructuredProperty(TableReferenceEntity)
    custom_parameters = ndb.TextProperty(indexed=False)

    STATUS_IN_PROGRESS = 'In Progress'
    STATUS_DONE = 'Done'
    STATUS_FAILED = 'Failed'

    @classmethod
    def create(cls, source_table, target_table, custom_parameters=None):
        return RestoreItem(
            restoration_job_key=None,
            status=cls.STATUS_IN_PROGRESS,
            completed=None,
            source_table=TableReferenceEntity.create(source_table),
            target_table=TableReferenceEntity.create(target_table),
            custom_parameters=json.dumps(custom_parameters)
        )

    @classmethod
    def get_restoration_items(cls, restoration_job_key):
        query = cls.query() \
            .filter(cls.restoration_job_key == restoration_job_key)
        for restore_item in query_using_cursor(query, 1000):
            yield restore_item

    @classmethod
    def get_by_key(cls, key):
        return key.get()

    @property
    def source_table_reference(self):
        return TableReference(
            project_id=self.source_table.project_id,
            dataset_id=self.source_table.dataset_id,
            table_id=self.source_table.table_id,
            partition_id=self.source_table.partition_id
        )

    @property
    def target_table_reference(self):
        return TableReference(
            project_id=self.target_table.project_id,
            dataset_id=self.target_table.dataset_id,
            table_id=self.target_table.table_id,
            partition_id=self.target_table.partition_id
        )

    @classmethod
    def update_with_done(cls, restore_item_key):
        cls.__update_restore_item(restore_item_key, cls.STATUS_DONE)

    @classmethod
    def update_with_failed(cls, restore_item_key, error_message):
        cls.__update_restore_item(restore_item_key, cls.STATUS_FAILED,
                                  error_message)

    @classmethod
    @ndb.transactional
    def __update_restore_item(cls, restore_item_key, status,
                              status_message=None):
        restore_item = cls.get_by_key(restore_item_key)

        not_updatable_statuses = (
            cls.STATUS_DONE, cls.STATUS_FAILED)

        if restore_item.status not in not_updatable_statuses:
            restore_item.status = status
            restore_item.status_message = status_message
            restore_item.completed = datetime.utcnow()
            restore_item.put()
        else:
            logging.warning(
                "Attempt to override one of the final statuses. Final status: %s, proposed status: %s. ",
                restore_item.status, status)
