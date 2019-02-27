import logging
import os
import time

from google.oauth2 import service_account


def create_credentials(service_account_filename):
    key_store = get_key_store()
    key_file = key_store + service_account_filename

    credentials = service_account.Credentials.from_service_account_file(key_file)
    scoped_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])
    logging.info("Service account email: %s", credentials.service_account_email)
    return scoped_credentials


def get_key_store():
    if 'KEY_STORE' in os.environ:
        return os.environ['KEY_STORE']
    else:
        return 'tests/smoke/'


def sleep(seconds):
    logging.info("Waiting %ss...", seconds)
    time.sleep(seconds)
