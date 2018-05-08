from commons.secured_handler import SecuredHandler
from src.configuration import configuration


class BbqAuthenticatedHandler(SecuredHandler):

    def __init__(self, request=None, response=None):
        super(BbqAuthenticatedHandler, self). __init__(request, response)

    def get_authorized_requestor_service_accounts(self):
        return configuration.authorized_requestor_service_accounts
