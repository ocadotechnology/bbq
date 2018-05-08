import logging

import webapp2
from src.restore.test.restore_test import RestoreTest


class RestoreTestHandler(webapp2.RequestHandler):
    def __init__(self, request=None, response=None):
        super(RestoreTestHandler, self).__init__(request, response)

    def get(self):
        try:
            resp_msg = RestoreTest().test(self.request.host_url)
        except Exception as e:
            logging.exception(e.message)
            self.response.set_status(500)
            self.response.write(e.message)
        else:
            self.response.set_status(200)
            self.response.write(resp_msg)


app = webapp2.WSGIApplication([
    ('/restore/test', RestoreTestHandler)
])


