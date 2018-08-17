import os
import unittest

from apiclient.errors import HttpError
from google.appengine.ext import testbed
from mock import patch, Mock, mock, call

from src.commons.test_utils import utils
from mock.mock import PropertyMock
from src.commons import request_correlation_id
from src.backup.backup_scheduler import BackupScheduler


@patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
@patch('src.commons.error_reporting.ErrorReporting.__init__', Mock(return_value=None))
class TestBackupScheduler(unittest.TestCase):
    http_error_mock = HttpError(mock.Mock(status=500), 'Internal Error')

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.task_queue_stub = utils.init_testbed_queue_stub(self.testbed)

    def tearDown(self):
        self.testbed.deactivate()

    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids.return_value',
           ['dataset_id'])
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids.return_value',
           ['this-project-id-should-be-ignored'])
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    def test_dev_environment_happy_path(self, _, _1, _2):
        # given
        with patch('src.commons.config.configuration.Configuration.backup_settings_custom_project_list', new_callable=PropertyMock) as custom_project_list:
            custom_project_list.return_value = ['dev-project-1', 'dev-project-2']

            # when
            BackupScheduler().iterate_over_all_datasets_and_schedule_backups()

            # then
            tasks = self.task_queue_stub.get_filtered_tasks(
                queue_names='backup-scheduler')
            header_value = tasks[0].headers[request_correlation_id.HEADER_NAME]
            self.assertEqual(len(tasks), 2, "Should create one task in queue")
            self.assertEqual(tasks[0].payload, 'projectId=dev-project-1&datasetId=dataset_id')
            self.assertEqual(tasks[1].payload, 'projectId=dev-project-2&datasetId=dataset_id')
            self.assertEqual(header_value, 'correlation-id')

    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids.return_value',
           ['dataset_id'])
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids.return_value',
           ['prod-project-id'])
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    @patch.object(request_correlation_id, 'get',
                  return_value='correlation-id')
    def test_non_dev_environment_happy_path(self, _, _1, _2):
        # given
        with patch('src.commons.config.configuration.Configuration.backup_settings_custom_project_list', new_callable=PropertyMock) as custom_project_list:
            custom_project_list.return_value = []

            # when
            BackupScheduler().iterate_over_all_datasets_and_schedule_backups()

            # then
            tasks = self.task_queue_stub.get_filtered_tasks(
                queue_names='backup-scheduler')
            header_value = tasks[0].headers[request_correlation_id.HEADER_NAME]
            self.assertEqual(len(tasks), 1, "Should create one task in queue")
            self.assertEqual(tasks[0].payload, 'projectId=prod-project-id&datasetId=dataset_id')
            self.assertEqual(header_value, 'correlation-id')

    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids.return_value',
           ['project1', 'project2'])
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids.side_effect',
           http_error_mock)
    @patch('src.commons.error_reporting.ErrorReporting.report')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    def test_scheduler_wont_stop_if_it_cant_list_datasets_for_any_project(
            self, _, _1, report):
        # given
        with patch('src.commons.config.configuration.Configuration.backup_settings_custom_project_list', new_callable=PropertyMock) as custom_project_list:
            custom_project_list.return_value = []

            # when
            BackupScheduler().iterate_over_all_datasets_and_schedule_backups()

            # then
            error_message = 'Failed to list and backup datasets: ' + \
                            str(TestBackupScheduler.http_error_mock)
            report.assert_has_calls([call(error_message), call(error_message)])

    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids.return_value',
           ['this-project-will-fail', 'this-project-will-be-scheduled'])
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids.side_effect',
           [http_error_mock, ['dataset2']])
    @patch('src.commons.error_reporting.ErrorReporting.report')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    @patch('src.commons.big_query.big_query.BigQuery.list_dataset_ids')
    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    def test_second_project_is_scheduled_even_if_first_failed(
            self, _, _1, _2, report):
        # given
        with patch('src.commons.config.configuration.Configuration.backup_settings_custom_project_list', new_callable=PropertyMock) as custom_project_list:
            custom_project_list.return_value = []

            # when
            BackupScheduler().iterate_over_all_datasets_and_schedule_backups()

            # then
            report.assert_called_once()

            tasks = self.task_queue_stub.get_filtered_tasks(
                queue_names='backup-scheduler')
            header_value = tasks[0].headers[request_correlation_id.HEADER_NAME]
            self.assertEqual(len(tasks), 1, "Should create only one task in queue")
            self.assertEqual(tasks[0].payload, 'projectId=this-project-will-be-scheduled&datasetId=dataset2')

            self.assertEqual(header_value, 'correlation-id')
