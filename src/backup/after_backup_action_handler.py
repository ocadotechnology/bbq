import logging

import webapp2

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.decorators.retry import retry
from src.commons.exceptions import JsonNotParseableException, \
    WrongJsonFormatException
from src.commons.handlers.json_handler import JsonHandler
from src.commons.big_query.copy_job_async.copy_job_result import CopyJobResult
from src.backup.datastore.Backup import Backup
from src.backup.datastore.Table import Table
from src.commons.big_query.big_query import BigQuery
from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.config.configuration import configuration
from src.commons.error_reporting import ErrorReporting
from src.commons.handlers.json_request_helper import JsonRequestHelper
from src.commons.table_reference import TableReference


class DatastoreTableGetRetriableException(Exception):
    pass


class AfterBackupActionHandler(JsonHandler):
    def __init__(self, request=None, response=None):
        super(AfterBackupActionHandler, self).__init__(request, response)
        self.BQ = BigQuery()

    def post(self, **_):
        request_body_json = JsonRequestHelper.parse_request_body(self.request.body)
        self.__validate(request_body_json)
        self.__process(request_body_json)
        self._finish_with_success()

    def __process(self, request_body_json):
        copy_job_results = CopyJobResult(request_body_json.get('jobJson'))
        data = request_body_json.get('data')

        if copy_job_results.has_errors():
            error_message = "Copy job failed with errors: {} ." \
                            "Backup for source: {}, target: {} " \
                            "has not been done. " \
                .format(copy_job_results.error_message,
                        data["sourceBqTable"], data["targetBqTable"])
            ErrorReporting().report(error_message)
            return

        backup_table_metadata = BigQueryTableMetadata.get_table_by_big_query_table(
            copy_job_results.target_bq_table
        )

        if backup_table_metadata.table_exists():
            self.__create_backup(backup_table_metadata,
                                 copy_job_results)
            if backup_table_metadata.has_partition_expiration():
                self.__disable_partition_expiration(
                    TableReference.from_bq_table(copy_job_results.target_bq_table))
        else:
            pass
            ErrorReporting().report(
                "Backup table {0} not exist. Backup entity is not created".format(
                    copy_job_results.target_bq_table))

    def __disable_partition_expiration(self, backup_table_reference):
        self.BQ.disable_partition_expiration(
            backup_table_reference.project_id,
            backup_table_reference.dataset_id,
            backup_table_reference.table_id
        )

    @staticmethod
    @retry(DatastoreTableGetRetriableException, tries=6, delay=4, backoff=2)
    def __create_backup(backup_table_metadata, copy_job_results):

        table_entity = Table.get_table_by_reference(
            TableReference.from_bq_table(copy_job_results.source_bq_table)
        )

        if table_entity is None:
            raise DatastoreTableGetRetriableException()

        backup = Backup(
            parent=table_entity.key,
            last_modified=copy_job_results.start_time,
            created=copy_job_results.end_time,
            dataset_id=copy_job_results.target_dataset_id,
            table_id=copy_job_results.target_table_id,
            numBytes=backup_table_metadata.table_size_in_bytes()
        )
        logging.debug(
            "Saving backup to datastore, source:{0}, target:{1}".format(
                copy_job_results.source_bq_table,
                copy_job_results.target_bq_table))

        backup.insert_if_absent(backup)

    @staticmethod
    def __validate(_json):
        if 'jobJson' not in _json:
            raise WrongJsonFormatException("JSON has no jobJson parameter")

        data = _json.get('data')
        if 'sourceBqTable' not in data or 'targetBqTable' not in data:
            raise WrongJsonFormatException(
                "JSON has no sourceBqTable or targetBqTable parameters"
            )

    @staticmethod
    def __create_table_reference(json_dict):
        try:
            table_id, partition_id = BigQueryTable \
                .split_table_and_partition_id(json_dict['table_id'])
            return TableReference(project_id=json_dict['project_id'],
                                  dataset_id=json_dict['dataset_id'],
                                  table_id=table_id,
                                  partition_id=partition_id)
        except KeyError, e:
            raise JsonNotParseableException("Unable to find element: {}"
                                            .format(e.message))


app = webapp2.WSGIApplication([webapp2.Route(
    '/callback/backup-created/<project_id>/<dataset_id>/<table_id>',
    AfterBackupActionHandler
)], debug=configuration.debug_mode)
