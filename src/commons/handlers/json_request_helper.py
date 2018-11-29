import json

from src.commons.exceptions import JsonNotParseableException


class JsonRequestHelper(object):
    @staticmethod
    def parse_request_body(request_body):
        try:
            return json.loads(request_body)
        except ValueError, e:
            raise JsonNotParseableException(e.message)
