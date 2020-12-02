import webapp2

HEADER_NAME = 'request-correlation-id'


def get():
    request = webapp2.get_request()
    if HEADER_NAME in request.headers:
        return request.headers[HEADER_NAME]
    else:
        return None


def set_correlation_id(correlation_id):
    request = webapp2.get_request()
    request.headers[HEADER_NAME] = correlation_id
