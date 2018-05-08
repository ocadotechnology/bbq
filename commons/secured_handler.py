import logging

import webapp2

from oauth2_account import OAuth2Account


class MissingAuthorizationInHeaderException(Exception):
    pass


class ServiceAccountNotAuthorizedException(Exception):
    pass


class SecuredHandler(webapp2.RequestHandler):

    def __init__(self, request=None, response=None):
        super(SecuredHandler, self). __init__(request, response)

    def dispatch(self):
        try:
            self._assure_requestor_is_authorized()
            return super(SecuredHandler, self).dispatch()
        except MissingAuthorizationInHeaderException as e:
            self._finish_with_warning(response_code=401, msg=e.message)
        except ServiceAccountNotAuthorizedException as e:
            self._finish_with_warning(response_code=403, msg=e.message)
        except Exception as e:
            self.handle_exception(e, False)

    def _assure_requestor_is_authorized(self):
        if 'Authorization' not in self.request.headers:
            raise MissingAuthorizationInHeaderException(
                'Missing Authorization request header')

        try:
            email = OAuth2Account(
                self.request.headers['Authorization']
            ).get_email()
        except Exception as e:
            logging.error(
                "Error while extracting email from Authorization token"
            )
            raise e

        if email in self.get_authorized_requestor_service_accounts():
            return True
        else:
            raise ServiceAccountNotAuthorizedException(
                'Requestor not authorized'
            )

    def get_authorized_requestor_service_accounts(self):
        raise Exception("NotImplementedException")

    def _finish_with_success(self, msg):
        logging.info(msg)
        self.response.set_status(200)
        self.response.write(msg)

    def _finish_with_warning(self, response_code, msg):
        logging.warning(msg)
        self.response.set_status(response_code, msg)
        self.response.write(msg)

    def _finish_with_error(self, response_code, msg):
        logging.exception(msg)
        self.response.set_status(response_code, msg)
        self.response.write(msg)

    def handle_exception(self, exception, debug): # nopep8 pylint: disable=W0613
        unknown_msg = 'Problem occurred while processing request. ' \
              'Please try again.'
        self._finish_with_error(500, msg=unknown_msg)
