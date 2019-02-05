import json
import logging

import webapp2

from src.commons.big_query.copy_job_async.result_check.result_check import \
    ResultCheck
from src.commons.big_query.copy_job_async.result_check.result_check_request import \
    ResultCheckRequest
from src.commons.config.configuration import configuration


class ResultCheckHandler(webapp2.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ResultCheckHandler, self).__init__(request, response)

    def post(self):
        result_check_request = self.__deserialize_result_check_request(self.request)
        logging.info("Running copy job result check request: '%s'", result_check_request)
        assert result_check_request.retry_count >= 0
        ResultCheck().check(result_check_request)

    @staticmethod
    def __deserialize_result_check_request(request):
        result_check_request_json = json.loads(request.get("resultCheckRequest"))
        return ResultCheckRequest.from_json(result_check_request_json)


app = webapp2.WSGIApplication([webapp2.Route(
    '/tasks/copy_job_async/result_check',
    ResultCheckHandler
)], debug=configuration.debug_mode)
