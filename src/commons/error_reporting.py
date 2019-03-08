import logging
import traceback

import googleapiclient.discovery
from oauth2client.client import GoogleCredentials

from src.commons.config.environment import Environment
from src.commons.decorators.retry import retry

from google.appengine.api.app_identity import app_identity


class ErrorReporting(object):
    def __init__(self):
        self.service = Environment.version_id()
        self.logging_client = googleapiclient.discovery.build(
            'clouderrorreporting',
            'v1beta1',
            credentials=self._create_credentials(),
            http=self._create_http()
        )

    @staticmethod
    def _create_credentials():
        return GoogleCredentials.get_application_default()

    @staticmethod
    def _create_http():
        return None

    def report(self, message, caller=None):
        if not caller:
            stack = traceback.extract_stack()
            caller = stack[-2]
        file_path = caller[0]
        line_number = caller[1]
        function_name = caller[2]
        report_location = {
            'filePath': file_path,
            'lineNumber': line_number,
            'functionName': function_name
        }
        # pylint: disable=W1505
        logging.warn("Reporting error, "
                     "message: %s, "
                     "caller: %s",
                     message, caller
                    )

        try:
            self._send_error_report(message, report_location=report_location)
        except Exception:
            logging.exception("Unable to report error: %s", message)

    @retry(Exception, tries=3, delay=1, backoff=1)
    def _send_error_report(self, message,
                           report_location):
        payload = {
            'serviceContext': {
                'service': self.service,
            },
            'message': '{0}'.format(message),
            'context': {
                'reportLocation': report_location
            }
        }

        self.logging_client.projects().events().report(
            projectName='projects/{}'.format(app_identity.get_application_id()),
            body=payload).execute()
