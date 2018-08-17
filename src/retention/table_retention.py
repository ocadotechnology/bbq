import logging

from apiclient.errors import HttpError  # nopep8 pylint: disable=C0413
from google.appengine.ext import ndb

from src.backup.datastore.Backup import Backup
from src.commons.big_query.big_query import TableNotFoundException, BigQuery
from src.commons.config.configuration import configuration
from src.retention.should_perform_retention_predicate import \
    ShouldPerformRetentionPredicate
from src.commons.table_reference import TableReference


class TableRetention(object):

    def __init__(self, policy):
        self.big_query_service = BigQuery()
        self.policy = policy

    def perform_retention(self, table_reference, table_key):
        backups = Backup.get_all_backups_sorted(ndb.Key(urlsafe=table_key))
        logging.debug("Fetched %s backups for the table: %s", len(backups),
                      table_reference)

        if not ShouldPerformRetentionPredicate.test(backups):
            return

        logging.info("Retention policy used for table '%s': '%s'",
                     table_reference, type(self.policy).__name__)

        for backup in self.policy\
                .get_backups_eligible_for_deletion(backups=backups,
                                                   table_reference=table_reference):
            self.__delete_backup_in_bq_and_update_datastore(backup)

    def __delete_backup_in_bq_and_update_datastore(self, backup):
        try:
            table_reference = TableReference(configuration.backup_project_id,
                                             backup.dataset_id,
                                             backup.table_id)

            self.big_query_service.delete_table(table_reference)
            logging.debug("Table %s deleted from BigQuery. "
                          "Updating datastore. Retention policy used: '%s'",
                          table_reference,
                          type(self.policy).__name__)
            Backup.mark_backup_deleted(backup.key)
        except TableNotFoundException:
            Backup.mark_backup_deleted(backup.key)
            logging.warning(
                "Table '%s' was not found. But we updated datastore anyway",
                backup.table_id)
        except HttpError as ex:
            error_message = "Unexpected HttpError occurred while deleting " \
                            "table '{}', error: {}: {}"\
                .format(backup.table_id, type(ex), ex)
            logging.exception(error_message)
        except Exception as ex:
            error_message = "Could not delete backup '{}' error: {}: {}"\
                .format(backup.table_id, type(ex), ex)
            logging.exception(error_message)
