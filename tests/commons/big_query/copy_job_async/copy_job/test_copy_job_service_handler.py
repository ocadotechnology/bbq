import json
import os

from src.commons.big_query.copy_job_async.copy_job import \
    copy_job_service_handler
from src.commons.big_query.copy_job_async.copy_job.copy_job_request \
    import CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job.copy_job_service \
    import CopyJobService
from src.commons.big_query.copy_job_async.post_copy_action_request \
    import PostCopyActionRequest
from src.commons.encoders.request_encoder import RequestEncoder

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest

from src.commons.big_query.big_query_table import BigQueryTable
from google.appengine.ext import testbed
from mock import patch


class TestCopyJobServiceHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        app = copy_job_service_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(CopyJobService, 'run_copy_job_request')
    def test_happy_path(self, copy_table_mock):
        # given
        source_big_query_table = BigQueryTable("source_project_id",
                                               "source_dataset_id",
                                               "source_table_id")
        target_big_query_table = BigQueryTable("target_project_id",
                                               "target_dataset_id",
                                               "target_table_id")

        post_copy_action_request = PostCopyActionRequest(url="/my/url", data={"key1": "value1"})

        url = '/tasks/copy_job_async/copy_job'
        # when
        self.under_test.post(
            url=url,
            params={"copyJobRequest": json.dumps(
                CopyJobRequest(
                    task_name_suffix=None,
                    copy_job_type_id=None,
                    source_big_query_table=source_big_query_table,
                    target_big_query_table=target_big_query_table,
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_EMPTY",
                    retry_count=0,
                    post_copy_action_request=post_copy_action_request
                ), cls=RequestEncoder)}
        )

        # then
        copy_table_mock.assert_called_with(
            CopyJobRequest(
                task_name_suffix=None,
                copy_job_type_id=None,
                source_big_query_table=source_big_query_table,
                target_big_query_table=target_big_query_table,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_EMPTY",
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )

    @patch.object(CopyJobService, 'run_copy_job_request')
    def test_handling_of_remote_code_execution(self, _):
        # given
        url = '/tasks/copy_job_async/copy_job'

        payload_with_exploitation = \
            '{' \
            '"py/object": "list", ' \
            '"py/reduce": [{"py/type": "subprocess.Popen"}, ["exit 1"], null, null, null], ' \
            '"task_name_suffix": null, ' \
            '"create_disposition": "CREATE_IF_NEEDED", ' \
            '"write_disposition": "WRITE_EMPTY", ' \
            '"retry_count": 0, ' \
            '"source_big_query_table": {"dataset_id": "source_dataset_id", "project_id": "source_project_id", "table_id": "source_table_id"}, ' \
            '"target_big_query_table": {"dataset_id": "target_dataset_id", "project_id": "target_project_id", "table_id": "target_table_id"}, ' \
            '"post_copy_action_request": {"url": "/my/url", "data": {"key1": "value1"}}, ' \
            '"copy_job_type_id": null' \
            '}'

        # when && then
        self.under_test.post(
            url=url, params={"copyJobRequest": payload_with_exploitation})
