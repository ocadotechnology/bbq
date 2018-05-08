import json
import os

from src.restore.status import restoration_job_status_handler
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService

os.environ['SERVER_SOFTWARE'] = 'Development/'

import unittest

import webtest
from mock import patch


class TestRestorationJobStatusHandler(unittest.TestCase):
    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(restoration_job_status_handler.app)

    @patch.object(RestorationJobStatusService, 'get_restoration_job')
    def test_get_restoration_status(self, get_restoration_job):
        # given
        get_restoration_job.return_value = {'restorationJobId': '123-456',
                                            'status': 'SUCCESS',
                                            'itemsCount': 0,
                                            'restorationItems': []}

        # when
        response = self.under_test.get('/restore/jobs/123-456')

        # then
        get_restoration_job.assert_called_once_with('123-456', False)
        response_json = json.loads(response.body)
        self.assertTrue('restorationJobId' in response_json)

    @patch.object(RestorationJobStatusService, 'get_restoration_job')
    def test_get_restoration_status_should_pass_warnings_only_parameter(self,
                                                                        get_restoration_job):
        # given
        get_restoration_job.return_value = {'restorationJobId': '123-456',
                                            'status': 'SUCCESS',
                                            'itemsCount': 0,
                                            'restorationItems': []}

        # when
        response = self.under_test.get(
            '/restore/jobs/123-456?warningsOnly')

        # then
        get_restoration_job.assert_called_once_with('123-456', True)
        response_json = json.loads(response.body)
        self.assertTrue('restorationJobId' in response_json)

    def test_should_fail_when_restore_job_id_parameter_not_provided(self):
        # when
        response = self.under_test.get('/restore/jobs', expect_errors=True)

        # then
        self.assertEquals(404, response.status_int)
