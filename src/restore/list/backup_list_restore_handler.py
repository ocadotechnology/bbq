import json
import logging
import urllib
import uuid

import webapp2

from commons.exceptions import ParameterValidationException, \
    JsonNotParseableException
from commons.json_handler import JsonHandler
from src.environment import Environment
from src.bbq_authenticated_handler import BbqAuthenticatedHandler
from src.big_query import validators
from src.big_query.validators import WrongDatasetNameException
from src.restore.list.backup_key_parser import BackupKeyParser
from src.restore.list.backup_list_restore_service import \
    BackupListRestoreService, BackupItem, BackupListRestoreRequest
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class BackupListRestoreHandler(JsonHandler):
    def post(self):
        target_dataset_id = self.request.get('targetDatasetId', None)
        self.__validate_params(target_dataset_id)
        body_json = self.__parse_body_json()
        self.__validate_body_json(body_json)

        backup_items = self.__create_backup_items(body_json)
        self.__validate_backup_items(backup_items)
        restore_request = BackupListRestoreRequest(
            backup_items, target_dataset_id
        )

        restoration_job_id = str(uuid.uuid4())
        logging.info("Created restoration_job_id: %s", restoration_job_id)

        BackupListRestoreService().restore(restoration_job_id, restore_request)

        restore_data = {
            'restorationJobId': restoration_job_id,
            'restorationStatusEndpoint': RestorationJobStatusService.get_status_endpoint(
                restoration_job_id),
            'restorationWarningsOnlyStatusEndpoint': RestorationJobStatusService.get_warnings_only_status_endpoint(
                restoration_job_id)
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

    @staticmethod
    def __validate_params(target_dataset_id):
        try:
            if target_dataset_id:
                validators.validate_dataset_id(target_dataset_id)
        except WrongDatasetNameException, e:
            raise ParameterValidationException(e.message)


class BackupListRestoreAuthenticatedHandler(BackupListRestoreHandler,
                                            BbqAuthenticatedHandler):

    def __init__(self, request=None, response=None):
        super(BackupListRestoreHandler, self). \
            __init__(request, response)


app = webapp2.WSGIApplication([
    webapp2.Route('/restore/list', BackupListRestoreHandler),
    webapp2.Route('/schedule/restore/list',
                  BackupListRestoreAuthenticatedHandler),

], debug=Environment.is_debug_mode_allowed())
