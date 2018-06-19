import logging

import jsonpickle
import webapp2

from src.backup.copy_job_async.copy_job.copy_job_service import CopyJobService
from src.configuration import configuration


class CopyJobServiceHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(CopyJobServiceHandler, self).__init__(request, response)

    def post(self):
        copy_job_request = self.__deserialize_copy_job_request(self.request)
        logging.info("Executing copy Job: '%s'", copy_job_request)
        CopyJobService.run_copy_job_request(copy_job_request)

    @staticmethod
    def __deserialize_copy_job_request(request):
        return jsonpickle.decode(request.get("copyJobRequest"))

app = webapp2.WSGIApplication([
    ('/tasks/copy_job_async/copy_job', CopyJobServiceHandler)
], debug=configuration.debug_mode)
