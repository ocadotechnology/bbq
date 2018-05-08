import os

import jsonpickle

from src.backup.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest
from src.backup.copy_job_async.result_check import result_check_handler
from src.backup.copy_job_async.result_check.result_check import \
    ResultCheck
from src.backup.copy_job_async.result_check.result_check_request import ResultCheckRequest

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch



class TestResultCheckHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        app = result_check_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(ResultCheck, 'check')
    def test_happy_path(self, result_check_mock):
        # given
        project_id = "target_project_id"
        job_id = "job_id"
        retry_count = "1"
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        result_check_request = self.create_example_result_check_request(
            project_id, job_id, retry_count, post_copy_action_request)

        # when
        self.under_test.post(url='/tasks/copy_job_async/result_check', params={"resultCheckRequest": jsonpickle.encode(result_check_request)})

        # then
        result_check_mock.assert_called_with(
            self.create_example_result_check_request(
                project_id, job_id, retry_count, post_copy_action_request)
        )

    def create_example_result_check_request(self, project_id, job_id,
                                            retry_count,
                                            post_copy_action_request):
        return ResultCheckRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            project_id=project_id,
            job_id=job_id,
            retry_count=retry_count,
            post_copy_action_request=post_copy_action_request
        )
