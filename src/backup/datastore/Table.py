import logging

from datetime import timedelta, date
from google.appengine.ext import ndb

from src.backup.datastore.Backup import Backup
from src.commons.decorators.retry import retry
from src.commons.table_reference import TableReference


class Table(ndb.Model):
    project_id = ndb.StringProperty(indexed=True)
    dataset_id = ndb.StringProperty(indexed=True)
    table_id = ndb.StringProperty(indexed=True)
    last_checked = ndb.DateTimeProperty(indexed=True)
    partition_id = ndb.StringProperty(indexed=True)

    @classmethod
    @retry(Exception, tries=5, delay=1, backoff=2)
    def create(cls, project_id, dataset_id, table_id,
               partition_id, last_checked):

        table_reference = TableReference(project_id, dataset_id,
                                         table_id, partition_id)
        logging.info("Creating table entity for %s", table_reference)
        table_entity = Table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            partition_id=partition_id,
            last_checked=last_checked
        )
        table_entity.put()

    @retry(Exception, tries=5, delay=1, backoff=2)
    def update_last_check(self, last_checked):
        table_reference = TableReference(self.project_id, self.dataset_id,
                                         self.table_id, self.partition_id)
        logging.info("Updating last_check in entity entity for %s",
                     table_reference)
        self.last_checked = last_checked
        self.put()

    @classmethod
    def get_table_from_backup(cls, backup):
        return backup.key.parent().get()

    @classmethod
    def get_table_by_reference(cls, table_reference):
        return cls.get_table(table_reference.project_id,
                             table_reference.dataset_id,
                             table_reference.table_id,
                             table_reference.partition_id)

    @classmethod
    @retry(Exception, tries=5, delay=1, backoff=2)
    def get_table(cls, project_id, dataset_id, table_id, partition_id=None):
        result_table = cls.query() \
            .filter(Table.project_id == project_id) \
            .filter(Table.dataset_id == dataset_id) \
            .filter(Table.table_id == table_id) \
            .filter(Table.partition_id == partition_id) \
            .get()

        logging.info("Asking for %s, returned: %s",
                     TableReference(project_id, dataset_id, table_id,
                                    partition_id), result_table)
        return result_table

    @classmethod
    def get_tables(cls, project_id, dataset_id, page_size=1000):
        query = cls.query() \
            .filter(Table.project_id == project_id) \
            .filter(Table.dataset_id == dataset_id) \
            .order(-Table.partition_id)
        for table in query_using_cursor(query, page_size):
            yield table

    @classmethod
    def get_tables_with_max_partition_days(cls, project_id, dataset_id,
                                           max_partition_days,
                                           page_size=1000):
        dataset_query = cls.query() \
            .filter(Table.project_id == project_id) \
            .filter(Table.dataset_id == dataset_id)

        query_max_partitions = cls. \
            __add_max_partition_days_filter(dataset_query,
                                            max_partition_days)
        query_max_partitions = query_max_partitions.order(-Table.partition_id)
        for table in query_using_cursor(query_max_partitions, page_size):
            yield table

        query_non_partitioned = dataset_query.filter(Table.partition_id == None)
        for table in query_using_cursor(query_non_partitioned, page_size):
            yield table

    @property
    def last_backup(self):
        return self.__last_backup_async.get_result()

    @property
    def __last_backup_async(self):
        return Backup.query(ancestor=self.key) \
            .filter(Backup.deleted == None) \
            .order(-Backup.created) \
            .get_async()

    @staticmethod
    def get_last_backup_for_tables(table_entities):
        futures = []
        for table_entity in table_entities:
            last_backup_future = table_entity.__last_backup_async
            futures.append([table_entity, last_backup_future])
        for table_entity, last_backup_future in futures:
            yield (table_entity, last_backup_future.get_result())

    def last_backup_not_newer_than(self, datetime):
        return Backup.query(ancestor=self.key) \
            .filter(Backup.deleted == None) \
            .filter(Backup.created <= datetime) \
            .order(-Backup.created).get()

    @classmethod
    def __add_max_partition_days_filter(cls, query, max_partition_days):
        oldest_partition_id = cls.__create_oldest_partition_id_in_range(
            max_partition_days)
        logging.debug("Oldest partition id to get %s", oldest_partition_id)

        return query.filter(Table.partition_id >= oldest_partition_id)

    @classmethod
    def __create_oldest_partition_id_in_range(cls, no_days_back):
        oldest_partition_date = date.today() - timedelta(days=no_days_back - 1)
        return oldest_partition_date.strftime("%Y%m%d")


# @refactor move it to common/util file instead of Table.py file. find good name
def query_using_cursor(query, page_size):
    ctx = ndb.get_context()
    ctx.set_cache_policy(False)
    results, cursor, more = query.fetch_page(page_size)
    for result in results:
        yield result
    while more and cursor:
        ctx.clear_cache()
        results, cursor, more = query.fetch_page(page_size, start_cursor=cursor)
        for result in results:
            yield result
