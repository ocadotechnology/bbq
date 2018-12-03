import json
import os
import uuid

from src.commons.exceptions import ParameterValidationException
from src.restore.dataset.dataset_restore_service import DatasetRestoreService
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService

os.environ['SERVER_SOFTWARE'] = 'Development/'
from src.restore.dataset import dataset_restore_handler

import unittest

import webtest
from mock import patch


class TestDatasetRestoreHandler(unittest.TestCase):
    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(dataset_restore_handler.app)

    @patch.object(DatasetRestoreService, 'restore')
    @patch.object(uuid, 'uuid4', return_value=123)
    @patch.object(RestorationJobStatusService,
                  'get_warnings_only_status_endpoint',
                  return_value="http://bbq_endpoint.com/restore/jobs/123?warningsOnly")
    @patch.object(RestorationJobStatusService, 'get_status_endpoint',
                  return_value="http://bbq_endpoint.com/restore/jobs/123")
    def test_regular_restore_request(self, get_status_endpoint,
                                     get_warnings_only_status_endpoint, _,
                                     restore):
        # given
        expected_response_body = {
            "restorationJobId": "123",
            "projectId": "p123",
            "datasetId": "d123",
            "restorationStatusEndpoint": "http://bbq_endpoint.com/restore/jobs/123",
            "restorationWarningsOnlyStatusEndpoint": "http://bbq_endpoint.com/restore/jobs/123?warningsOnly"
        }

        # when
        response = self.under_test.post(
            '/restore/project/p123/dataset/d123',
            params={
                'isRestoreToSourceProject': True,
                'targetDatasetId': 'td123',
                'createDisposition': 'CREATE_IF_NEEDED',
                'writeDisposition': 'WRITE_EMPTY',
                'maxPartitionDays': '100'})

        # then
        get_status_endpoint.assert_called_once_with("123")
        get_warnings_only_status_endpoint.assert_called_once_with("123")

        restore.assert_called_once_with(
            restoration_job_id="123",
            project_id='p123',
            dataset_id='d123',
            target_project_id='p123',
            target_dataset_id='td123',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_EMPTY',
            max_partition_days=100
        )
        self.assertEqual(response.body, json.dumps(expected_response_body))

    @patch.object(DatasetRestoreService, 'restore',
                  side_effect=ParameterValidationException)
    @patch.object(uuid, 'uuid4', return_value=123)
    def test_return_400_if_parameter_validation_exception(self, _, _1):
        # when
        response = self.under_test.post('/restore/project/p123/dataset/d123',
                                        expect_errors=True)

        # then
        self.assertEqual(response.status_int, 400)

    @patch.object(uuid, 'uuid4', return_value=123)
    def test_return_400_if_not_valid_dataset_value_in_url(self, _):
        # when
        response = self.under_test.post(
            '/restore/project/p123/dataset/dataset-with-dash',
            expect_errors=True)

        # then
        self.assertEqual(response.status_int, 400)

    @patch.object(uuid, 'uuid4', return_value=123)
    def test_return_400_if_not_valid_target_dataset_parameter(self, _):
        # when
        response = self.under_test.post('/restore/project/p123/dataset/d123',
                                        params={
                                            'targetDatasetId': 'dataset-with-dash'},
                                        expect_errors=True)

        # then
        self.assertEqual(response.status_int, 400)

    @patch.object(uuid, 'uuid4', return_value=123)
    def test_return_400_if_not_valid_write_disposition(self, _):
        # when
        response = self.under_test.post('/restore/project/p123/dataset/d123',
                                        params={
                                            'writeDisposition': 'BAD_WRITE_DISPOSTION'},
                                        expect_errors=True)

        # then
        self.assertEqual(response.status_int, 400)

    @patch.object(uuid, 'uuid4', return_value=123)
    def test_return_400_if_not_valid_create_disposition(self, _):
        # when
        response = self.under_test.post('/restore/project/p123/dataset/d123',
                                        params={
                                            'createDisposition': 'BAD_CREATE_DISPOSTION'},
                                        expect_errors=True)

        # then
        self.assertEqual(response.status_int, 400)
