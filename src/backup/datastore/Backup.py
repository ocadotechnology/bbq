import logging

import datetime
from google.appengine.ext import ndb

from src.commons.decorators.retry import retry


class Backup(ndb.Model):

    # date of last modification that will be included in the Backup
    # (copyJob start time - as it is atomic operation
    # and every change before that point is included in copy)
    last_modified = ndb.DateTimeProperty(indexed=True)

    # date of backup table creation (copyJob end)
    created = ndb.DateTimeProperty(auto_now_add=True, indexed=True)

    table_id = ndb.StringProperty(indexed=True)

    dataset_id = ndb.StringProperty(indexed=True)

    numBytes = ndb.IntegerProperty(indexed=True)

    deleted = ndb.DateTimeProperty(indexed=True)

    @classmethod
    def get_all(cls):
        ctx = ndb.get_context()
        ctx.set_cache_policy(False)
        query = cls.query()
        more = True
        cursor = None
        while more:
            backups, cursor, more = query.fetch_page(1000, start_cursor=cursor)
            for backup in backups:
                yield backup

    @classmethod
    def get_by_key(cls, key):
        return key.get()

    def get_table(self):
        return self.key.parent().get()

    @classmethod
    def mark_backup_deleted(cls, key):
        backup = key.get()
        backup.deleted = datetime.datetime.utcnow()
        backup.put()

        logging.info(
            'Table retention: Deleting table with key:%s for table '
            'with tableId:%s, created:%s, bytes:%s',
            backup.key, backup.table_id, backup.created, backup.numBytes
        )

    # nopep8 pylint: disable=C0121
    @classmethod
    @retry(Exception, tries=5, delay=1, backoff=2)
    def get_all_backups_sorted(cls, ancestor_key):
        return Backup.query(ancestor=ancestor_key) \
            .filter(ndb.GenericProperty('deleted') == None) \
            .order(-Backup.created) \
            .fetch()

    @classmethod
    def sort_backups_by_create_time_desc(cls, backups):
        copy = list(backups)
        copy.sort(key=lambda b: b.created, reverse=True)
        return copy

    @classmethod
    @ndb.transactional
    def insert_if_absent(cls, backup_entity):
        at_least_one_exists = \
            Backup.query(ancestor=backup_entity.key.parent()) \
            .filter(Backup.dataset_id == backup_entity.dataset_id) \
            .filter(Backup.table_id == backup_entity.table_id) \
            .get()

        if at_least_one_exists:
            logging.info("Backup entity already exists in Datastore")
        else:
            backup_entity.put()
