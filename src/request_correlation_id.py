import webapp2

HEADER_NAME = 'request-correlation-id'


def get():
    request = webapp2.get_request()
    if HEADER_NAME in request.headers:
        return request.headers[HEADER_NAME]
    else:
        return None
