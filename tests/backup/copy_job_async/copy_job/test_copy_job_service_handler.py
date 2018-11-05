import os

import jsonpickle

from src.commons.copy_job_async.copy_job import copy_job_service_handler
from src.commons.copy_job_async.copy_job.copy_job_request import CopyJobRequest
from src.commons.copy_job_async.copy_job.copy_job_service import CopyJobService
from src.commons.copy_job_async.post_copy_action_request import \
    PostCopyActionRequest

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
            params={"copyJobRequest": jsonpickle.encode(
                CopyJobRequest(
                    task_name_suffix=None,
                    copy_job_type_id=None,
                    source_big_query_table=source_big_query_table,
                    target_big_query_table=target_big_query_table,
                    retry_count=0,
                    post_copy_action_request=post_copy_action_request
                )
            )}
        )

        # then
        copy_table_mock.assert_called_with(
            CopyJobRequest(
                task_name_suffix=None,
                copy_job_type_id=None,
                source_big_query_table=source_big_query_table,
                target_big_query_table=target_big_query_table,
                retry_count=0,
                post_copy_action_request=post_copy_action_request
            )
        )
