import logging

import webapp2
from google.appengine.ext import ndb
from google.appengine.ext.ndb import Cursor

from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class BackupEntityMigrationHandler(webapp2.RequestHandler):

    def get(self):

        table_cursor_value = self.request.get('cursor', None)
        if table_cursor_value:
            table_cursor = Cursor(urlsafe=table_cursor_value)
        else:
            table_cursor = None
        logging.info("Migration of Backup entities was started. Table_cursor: %s",
                     table_cursor)

        self.schedule_backup_entities(table_cursor)

    def schedule_backup_entities(self, table_cursor):
        tables, next_cursor, more = Table.get_all_with_cursor(table_cursor)

        Tasks.schedule('entity-backup-migration-worker',
                       self.__create_table_backups_migration_tasks(tables))

        if more and next_cursor:
            Tasks.schedule('entity-backup-migration-scheduler', Tasks.create(
                method='GET',
                url='/backup_entity/migrate',
                params={'cursor': next_cursor.urlsafe()},
                name='migrationSchedule_' + str(next_cursor.urlsafe()).replace(
                    '=', '_').replace('+', '__').replace('/', '___')))

    def __create_table_backups_migration_tasks(self, tables):
        return [
            Tasks.create(
                method='GET',
                url='/backup_entity/migrate_backup_for_table',
                params={'table_key': table.key.urlsafe()},
                name='migrate_backup_for_table_' + str(
                    table.key.urlsafe()).replace(
                    '=', '_').replace('+', '__').replace('/', '___'))
            for table in tables
        ]


class BackupEntityMigrationWorkerHandler(webapp2.RequestHandler):

    def get(self):
        table_key = self.request.get('table_key', None)
        self.migrate_backup_entities(ndb.Key(urlsafe=table_key))

    @ndb.transactional
    def migrate_backup_entities(self, table_key):
            backups = Backup.get_all_backups_entities(table_key)
            logging.info("Migrating backups: %s", backups)
            ndb.put_multi(backups, use_cache=False)


app = webapp2.WSGIApplication([
    ('/backup_entity/migrate', BackupEntityMigrationHandler),
    ('/backup_entity/migrate_backup_for_table', BackupEntityMigrationWorkerHandler)
], debug=configuration.debug_mode)
