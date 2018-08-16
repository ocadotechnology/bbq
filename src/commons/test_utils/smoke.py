import logging
import os
import time
import httplib2

from oauth2client import service_account


def create_http_client(service_account_filename):
    scopes = ['https://www.googleapis.com/auth/userinfo.email']
    credentials = create_credentials(service_account_filename, scopes)
    return credentials.authorize(httplib2.Http(timeout=60))


def create_credentials(service_account_filename, scopes=''):
    key_store = get_key_store()
    key_file = key_store + service_account_filename

    logging.info("Authorizing client from credentials: %s, %s",
                 key_file, scopes)
    credentials = service_account.ServiceAccountCredentials\
        .from_json_keyfile_name(key_file, scopes)
    logging.info("Service account email: %s", credentials.service_account_email)
    return credentials


def get_key_store():
    if 'KEY_STORE' in os.environ:
        return os.environ['KEY_STORE']
    else:
        return 'tests/smoke/'


def sleep(seconds):
    logging.info("Waiting %ss...", seconds)
    time.sleep(seconds)
