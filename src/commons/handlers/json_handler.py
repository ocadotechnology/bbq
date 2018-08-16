import json
import logging

import webapp2
from googleapiclient.errors import HttpError

from src.commons.exceptions import ParameterValidationException, \
    JsonNotParseableException, WrongJsonFormatException, NotFoundException


class JsonHandler(webapp2.RequestHandler):

    def _finish_with_success(self, json_object=None):
        if json_object is None:
            json_object = {'status': 'success'}
        self.response.headers['Content-Type'] = 'application/json'
        self.response.set_status(200)
        self.response.out.write(json.dumps(json_object))

    def _finish_with_error(self, response_code, msg):
        logging.error(msg)
        self.response.headers['Content-Type'] = 'application/json'
        obj = {'status': 'failed',
               'httpStatus': response_code,
               'message': msg}
        self.response.set_status(response_code)
        self.response.out.write(json.dumps(obj))

    def handle_exception(self, exception, debug):
        logging.exception(exception)
        if isinstance(exception, HttpError):
            self._finish_with_error(exception.resp.status, exception.message)
        elif isinstance(exception, ParameterValidationException):
            self._finish_with_error(400, exception.message)
        elif isinstance(exception, JsonNotParseableException):
            self._finish_with_error(400, exception.message)
        elif isinstance(exception, WrongJsonFormatException):
            self._finish_with_error(400, exception.message)
        elif isinstance(exception, NotFoundException):
            self._finish_with_error(404, exception.message)
        else:
            self._finish_with_error(500, exception.message)
