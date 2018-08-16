from src.commons.handlers.secured_handler import SecuredHandler
from src.commons.config.configuration import configuration


class BbqAuthenticatedHandler(SecuredHandler):

    def __init__(self, request=None, response=None):
        super(BbqAuthenticatedHandler, self). __init__(request, response)

    def get_authorized_requestor_service_accounts(self):
        return configuration.authorized_requestor_service_accounts
