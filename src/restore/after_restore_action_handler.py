
import webapp2
from google.appengine.ext import ndb

from src.commons.exceptions import WrongJsonFormatException
from src.commons.handlers.json_handler import JsonHandler
from src.commons.big_query.copy_job_async.copy_job_result import CopyJobResult
from src.commons.config.configuration import configuration
from src.commons.handlers.json_request_helper import JsonRequestHelper
from src.restore.datastore.restore_item import RestoreItem


class AfterRestoreActionHandler(JsonHandler):
    def __init__(self, request=None, response=None):
        super(AfterRestoreActionHandler, self).__init__(request, response)

    def post(self, **_):
        request_body_json = JsonRequestHelper.parse_request_body(self.request.body)
        self.__validate_json(request_body_json)

        url_safe_key = request_body_json.get('data').get('restoreItemKey')
        copy_job_result = CopyJobResult(request_body_json.get('jobJson'))

        restore_item_key = ndb.Key(urlsafe=url_safe_key)
        if copy_job_result.has_errors():
            error_message = copy_job_result.error_message
            RestoreItem.update_with_failed(restore_item_key, error_message)
        else:
            RestoreItem.update_with_done(restore_item_key)

        self._finish_with_success()

    @staticmethod
    def __validate_json(request_json):
        if 'data' not in request_json:
            raise WrongJsonFormatException(
                'JSON should have "data" element')
        if 'restoreItemKey' not in request_json.get('data'):
            raise WrongJsonFormatException(
                'JSON should have "restoreItemKey" element')
        if 'jobJson' not in request_json:
            raise WrongJsonFormatException(
                'JSON should have "jobJson" element')


app = webapp2.WSGIApplication([webapp2.Route(
    '/callback/restore-finished/',
    AfterRestoreActionHandler
)], debug=configuration.debug_mode)
