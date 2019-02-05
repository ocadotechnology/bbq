import json
import os

from src.commons.big_query.big_query_job_reference import BigQueryJobReference
from src.commons.big_query.copy_job_async.post_copy_action_request \
    import PostCopyActionRequest
from src.commons.big_query.copy_job_async.result_check import \
    result_check_handler
from src.commons.big_query.copy_job_async.result_check.result_check \
    import ResultCheck
from src.commons.big_query.copy_job_async.result_check.result_check_request \
    import ResultCheckRequest
from src.commons.encoders.request_encoder import RequestEncoder

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
        location = "EU"
        retry_count = "1"
        post_copy_action_request = \
            PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        result_check_request = self.create_example_result_check_request(
            project_id, job_id, location, retry_count, post_copy_action_request)

        # when
        self.under_test.post(
            url='/tasks/copy_job_async/result_check',
            params={"resultCheckRequest": json.dumps(result_check_request, cls=RequestEncoder)}
        )

        # then
        result_check_mock.assert_called_with(
            self.create_example_result_check_request(
                project_id, job_id, location, retry_count,
                post_copy_action_request)
        )

    @patch.object(ResultCheck, 'check')
    def test_handling_of_remote_code_execution(self, _):
        # given
        url = '/tasks/copy_job_async/result_check'

        payload_with_exploitation = \
            '{' \
            '"py/object": "list", ' \
            '"py/reduce": [{"py/type": "subprocess.Popen"}, ["exit 1"], null, null, null], ' \
            '"task_name_suffix": null, ' \
            '"copy_job_type_id": null, ' \
            '"job_reference": {"job_id": "job_id", "project_id": "source_project_id", "location": "location"}, ' \
            '"retry_count": 0, ' \
            '"post_copy_action_request": {"url": "/my/url", "data": {"key1": "value1"}}' \
            '}'

        # when && then
        self.under_test.post(
            url=url, params={"resultCheckRequest": payload_with_exploitation})

    @staticmethod
    def create_example_result_check_request(project_id, job_id, location,
                                            retry_count,
                                            post_copy_action_request):
        return ResultCheckRequest(
            task_name_suffix=None,
            copy_job_type_id=None,
            job_reference=BigQueryJobReference(project_id=project_id,
                                               job_id=job_id,
                                               location=location),
            retry_count=retry_count,
            post_copy_action_request=post_copy_action_request
        )
