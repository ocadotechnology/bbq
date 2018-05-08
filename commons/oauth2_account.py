import json
import logging
import httplib2

from googleapiclient.errors import HttpError

from commons.decorators.retry import retry


class OAuth2Account(object):
    def __init__(self, oauth_token):
        self.email = self.get_token_bearer_email(oauth_token)

    @classmethod
    @retry(HttpError, tries=6, delay=2, backoff=2)
    def get_token_bearer_email(cls, oauth_token):
        # pylint: disable=W0612
        context, response = httplib2.Http(). \
            request("https://www.googleapis.com/oauth2/v2/userinfo",
                    headers={'Host': 'www.googleapis.com',
                             'Authorization': oauth_token})
        logging.info('detected email for oauth token bearer: %s',
                     json.loads(response)['email'])
        return json.loads(response)['email']

    def get_email(self):
        return self.email
