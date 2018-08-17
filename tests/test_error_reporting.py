import inspect
import unittest

from mock import patch

from src.commons import error_reporting


# pylint: disable=W0212
class test_error_reporting(unittest.TestCase):
    def setUp(self):
        patch('src.commons.config.environment.Environment.version_id',
              return_value='dummy_version').start()
        patch('logging.warn').start()
        patch('googleapiclient.discovery.build').start()
        patch('src.commons.error_reporting.ErrorReporting._send_error_report',
              return_value=True).start()
        patch('oauth2client.client.GoogleCredentials.get_application_default') \
            .start()
        self.under_test = error_reporting.ErrorReporting()

    def tearDown(self):
        patch.stopall()

    # pylint: disable=E1136
    def test_should_report_error_when_caller_is_dynamic(self):
        # given
        message = 'message from test'
        file_that_reports_error = inspect.getabsfile(inspect.currentframe())
        method_that_reports_error = inspect.getframeinfo(
            inspect.currentframe()
        ).function
        # when
        self.under_test.report(message=message)
        caller = self.under_test._send_error_report.call_args
        # then
        self.assertEqual(
            file_that_reports_error,
            caller[1]['report_location']['filePath']
        )
        self.assertEqual(
            method_that_reports_error,
            caller[1]['report_location']['functionName']
        )
        self.assertEqual(message, caller[0][0])

    def test_should_report_error_when_caller_is_given(self):
        # given
        message = 'message from test'
        given_caller = [
            '/tmp/long/path/file.py',
            15,
            'some_weird_function_name'
        ]
        # when
        self.under_test.report(
            message=message,
            caller=given_caller
        )
        # then
        self.under_test._send_error_report.assert_called_once_with(
            message,
            report_location={
                'filePath': given_caller[0],
                'lineNumber': given_caller[1],
                'functionName': given_caller[2]
            }
        )
