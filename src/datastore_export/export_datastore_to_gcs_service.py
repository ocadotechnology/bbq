import datetime
import httplib
import json
import logging
import time

import webapp2
from google.appengine.api import app_identity
from google.appengine.api import urlfetch

from src.configuration import configuration


class ExportDatastoreToGCSService(webapp2.RequestHandler):
    @classmethod
    def invoke(cls, request, response):
        access_token, _ = app_identity.get_access_token(
            'https://www.googleapis.com/auth/datastore')
        app_id = configuration.backup_project_id
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
            headers=headers
        )

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
        self.operation_id = json.loads(operation).get("name")
        self.headers = headers

    def wait_till_done(self, timeout, period=60):
        url = "https://datastore.googleapis.com/v1/%s" % self.operation_id
        finish_time = time.time() + timeout

        while time.time() < finish_time:
            logging.info("Waiting %d seconds for request to end...", period)
            time.sleep(period)

            result = urlfetch.fetch(
                url=url,
                method=urlfetch.GET,
                deadline=60,
                headers=self.headers)
            content = json.loads(result.content)

            if "error" in content:
                error = content.get("error")
                logging.error("Request finished with errors: %s", error)
                return ExportDatastoreToGCSOperationResult(False)
            if content.get("done"):
                logging.info("Request finished.")
                output_url = content \
                    .get("metadata") \
                    .get("outputUrlPrefix")
                return ExportDatastoreToGCSOperationResult(True, output_url)
            logging.info("Request still in progress ...")

        logging.error("Timeout (%d seconds) exceeded !!!", timeout)
        return ExportDatastoreToGCSOperationResult(False)


class ExportDatastoreToGCSOperationResult(object):
    def __init__(self, finished_with_success, output_url_prefix=None):
        self.finished_with_success = finished_with_success
        self.output_url_prefix = output_url_prefix

    def is_finished_with_success(self):
        return self.finished_with_success

    def get_output_url_prefix(self):
        return self.output_url_prefix


app = webapp2.WSGIApplication([
    webapp2.Route('/cron/export-datastore-to-gcs', ExportDatastoreToGCSService)
], debug=configuration.debug_mode)
