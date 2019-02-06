import json
import logging
import urllib

import webapp2

from src.commons.config.configuration import configuration
from src.commons.exceptions import ParameterValidationException, \
    JsonNotParseableException
from src.commons.handlers import validators
from src.commons.handlers.json_handler import JsonHandler
from src.restore.list.backup_key_parser import BackupKeyParser
from src.restore.list.backup_list_restore_service import \
    BackupListRestoreService, BackupItem, BackupListRestoreRequest
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class BackupListRestoreHandler(JsonHandler):

    def post(self):

        is_restore_to_source_project = self.request.get('isRestoreToSourceProject', None)
        target_dataset_id = self.request.get('targetDatasetId', None)
        write_disposition = self.request.get('writeDisposition', None)
        create_disposition = self.request.get('createDisposition', None)

        target_project_id = None if is_restore_to_source_project \
            else configuration.default_restoration_project_id

        validators.validate_restore_request_params(
            target_project_id=target_project_id,
            target_dataset_id=target_dataset_id,
            create_disposition=create_disposition,
            write_disposition=write_disposition
        )

        body_json = self.__parse_body_json()
        self.__validate_body_json(body_json)

        backup_items = self.__create_backup_items(body_json)
        self.__validate_backup_items(backup_items)

        restore_request = BackupListRestoreRequest(backup_items,
                                                   target_project_id,
                                                   target_dataset_id,
                                                   create_disposition,
                                                   write_disposition)

        job_id = BackupListRestoreService().restore(restore_request)
        logging.info("Scheduled restoration job: %s", job_id)

        restore_data = {
            'restorationJobId': job_id,
            'restorationStatusEndpoint': RestorationJobStatusService.get_status_endpoint(job_id),
            'restorationWarningsOnlyStatusEndpoint': RestorationJobStatusService.get_warnings_only_status_endpoint(job_id)
        }
        self._finish_with_success(restore_data)

    def __parse_body_json(self):
        try:
            decoded_body = urllib.unquote(self.request.body).decode('utf8')
            logging.info("Request Body: '%s'", decoded_body)
            return json.loads(decoded_body)
        except ValueError, e:
            raise JsonNotParseableException(e.message)

    @staticmethod
    def __create_backup_items(request_body_json):
        backup_items = []
        for request_body_item in request_body_json:
            if "backupBqKey" in request_body_item:
                key = BackupKeyParser.parse_bq_key(
                    request_body_item["backupBqKey"])
            else:
                key = BackupKeyParser.parse_url_safe_key(
                    request_body_item["backupUrlSafeKey"])

            backup_items.append(BackupItem(backup_key=key,
                                           output_parameters=request_body_item))
        return backup_items

    @staticmethod
    def __validate_backup_items(backup_items):
        unique_backup_items = set()

        for backup_item in backup_items:
            if backup_item in unique_backup_items:
                raise ParameterValidationException(
                    "There are duplicated backup items - more than one "
                    "'backupUrlSafeKey' or 'backupBqKey' "
                    "are pointing to the same backup. "
                    "Duplicated backup item: {}".format(backup_item)
                )

            unique_backup_items.add(backup_item)

    @staticmethod
    def __validate_body_json(_json):
        for block in _json:
            if 'backupUrlSafeKey' not in block and 'backupBqKey' not in block:
                raise ParameterValidationException(
                    "There are missing 'backupUrlSafeKey' or 'backupBqKey' "
                    "variables in request body."
                )
            if 'backupUrlSafeKey' in block and 'backupBqKey' in block:
                raise ParameterValidationException(
                    "Please specify either 'backupUrlSafeKey' or 'backupBqKey' "
                    "element in single item."
                )


app = webapp2.WSGIApplication([
    webapp2.Route('/restore/list', BackupListRestoreHandler),
], debug=configuration.debug_mode)
