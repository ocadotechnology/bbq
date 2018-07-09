import datetime
import httplib
import json
import logging
import time

import webapp2

from google.appengine.api import app_identity
from google.appengine.api import urlfetch

from src.configuration import configuration


class ExportDatastoreToGCS(webapp2.RequestHandler):
    @classmethod
    def invoke(cls, request, response):
        access_token, _ = app_identity.get_access_token(
            'https://www.googleapis.com/auth/datastore')
        app_id = app_identity.get_application_id()
        url = 'https://datastore.googleapis.com/v1/projects/%s:export' % app_id

        output_url_prefix = cls.get_output_url_prefix(request)

        entity_filter = {
            'kinds': request.get_all('kind'),
            'namespace_ids': request.get_all('namespace_id')
        }
        request = {
            'project_id': app_id,
            'output_url_prefix': output_url_prefix,
            'entity_filter': entity_filter
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token
        }

        result = urlfetch.fetch(
            url=url,
            payload=json.dumps(request),
            method=urlfetch.POST,
            deadline=60,
            headers=headers)
        if result.status_code == httplib.OK:
            logging.info(result.content)
        elif result.status_code >= 500:
            logging.error(result.content)
        else:
            logging.warning(result.content)

        response.status_int = result.status_code
        return ExportDatastoreToGCSOperation(result.content, headers)

    @classmethod
    def get_output_url_prefix(cls, request):
        timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        output_url_prefix = request.get('gcs_bucket')
        assert output_url_prefix and output_url_prefix.startswith('gs://')
        if '/' not in output_url_prefix[5:]:
            # Only a bucket name has been provided - no prefix or trailing slash
            output_url_prefix += '/' + timestamp
        else:
            output_url_prefix += timestamp
        return output_url_prefix


class ExportDatastoreToGCSOperation(object):
    def __init__(self, operation, headers):
        self.operation = operation
        self.headers = headers

    def wait_till_done(self, timeout, period=60):
        app_id = app_identity.get_application_id()
        url = 'https://datastore.googleapis.com/v1/projects/{}/operations/{}'\
            .format(app_id, self.operation["name"])

        finish_time = time.time() + timeout
        while time.time() < finish_time:
            logging.info("Waiting %d seconds for request to end...", period)
            time.sleep(period)

            result = urlfetch.fetch(
                url=url,
                method=urlfetch.GET,
                deadline=60,
                headers=self.headers)

            loads = json.loads(result)

            if "error" in loads:
                error = loads.get("error")
                logging.error("Request finished with errors: %s", error)
                return ExportDatastoreToGCSOperationResult(False)
            if loads.get("done") in True:
                logging.info("Request finished.")
                output_url = loads["response"]["outputUrl"]
                return ExportDatastoreToGCSOperationResult(True, output_url)
            logging.info("Request still in progress ...")

        logging.error("Timeout (%d seconds) exceeded !!!", timeout)
        return ExportDatastoreToGCSOperationResult(False)


class ExportDatastoreToGCSOperationResult(object):
    def __init__(self, is_done, output_url_prefix=None):
        self.is_done = is_done
        self.output_url_prefix = output_url_prefix

    def is_done(self):
        return self.is_done

    def get_bucket_url(self):
        return self.output_url_prefix


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-gcs', ExportDatastoreToGCS)
], debug=configuration.debug_mode)
