import datetime
import logging

from google.appengine.api import memcache

from src.backup.backup_creator import BackupCreator
from src.backup.dataset_id_creator import DatasetIdCreator
from src.backup.datastore.Table import Table
from src.backup.default_backup_predicate import \
    DefaultBackupPredicate
from src.commons.config.configuration import configuration
from src.commons.table_reference import TableReference


class BackupProcess(object):
    def __init__(self, table_reference, big_query, big_query_table_metadata,
                 should_backup_predicate=DefaultBackupPredicate()):
        self.project_id = table_reference.get_project_id()
        self.dataset_id = table_reference.get_dataset_id()
        self.table_id = table_reference.get_table_id()
        self.partition_id = table_reference.get_partition_id()
        self.big_query = big_query
        self.big_query_table_metadata = big_query_table_metadata
        self.should_backup_predicate = should_backup_predicate
        self.now = None

    def start(self):
        self.now = datetime.datetime.utcnow()

        table_entity = Table.get_table(self.project_id, self.dataset_id,
                                       self.table_id, self.partition_id)

        if self.__backup_ever_done(table_entity):
            self.__update_last_check(table_entity)
            if self.__should_backup(table_entity):
                self.__create_backup(table_entity)
        else:
            if self.__should_backup(table_entity):
                table_entity = self.__create_table_entity()
                self.__create_backup(table_entity)

    @staticmethod
    def __backup_ever_done(table_entity):
        return table_entity is not None

    def __should_backup(self, table_entity):
        return self.should_backup_predicate.test(self.big_query_table_metadata,
                                                 table_entity)

    def __create_backup(self, table_entity):
        self.__ensure_dataset_for_backups_exists()
        BackupCreator(self.now) \
            .create_backup(table_entity, self.big_query_table_metadata)

    def __ensure_dataset_for_backups_exists(self):
        location = self.big_query_table_metadata.get_location()
        target_dataset_name = DatasetIdCreator.create(
            datetime.datetime.utcnow(), location, self.project_id)
        dataset_not_exists_in_cache = memcache.get(target_dataset_name) is None

        if dataset_not_exists_in_cache:
            destination_project_id = configuration.backup_project_id
            self.big_query.create_dataset(destination_project_id,
                                          target_dataset_name, location)
            memcache.add(target_dataset_name, 'exist')

    def __update_last_check(self, table_entity):
        logging.info("Updating last_check in entity entity for %s",
                     TableReference(self.project_id, self.dataset_id,
                                    self.table_id, self.partition_id))
        table_entity.last_checked = self.now
        table_entity.put()

    def __create_table_entity(self):
        logging.info(
            "Creating table entity for %s",
            TableReference(self.project_id, self.dataset_id,
                           self.table_id, self.partition_id))
        table_entity = Table(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            partition_id=self.partition_id,
            last_checked=self.now
        )
        table_entity.put()
        return table_entity
