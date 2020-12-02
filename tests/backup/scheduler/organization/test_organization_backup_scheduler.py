import os
import unittest

from google.appengine.ext import testbed, ndb
from mock import patch, PropertyMock

from src.backup.scheduler.organization.organization_backup_scheduler import \
    OrganizationBackupScheduler
from src.commons import request_correlation_id
from src.commons.config.configuration import Configuration
from src.commons.test_utils import utils


class TestOrganizationBackupScheduler(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_taskqueue_stub()
        self.testbed.init_taskqueue_stub(
            root_path=os.path.join(os.path.dirname(__file__), 'resources'))
        self.task_queue_stub = utils.init_testbed_queue_stub(self.testbed)
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    def test_organization_backup_scheduler_should_schedule_project_scheduler_tasks(
        self, list_project_ids, _1):
        # given
        list_project_ids.return_value = (['project-id-1', 'project-id-2'], None)
        # when
        OrganizationBackupScheduler().schedule_backup()

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[0].payload,
                         'projectId=project-id-1&pageToken=None')
        self.assertEqual(tasks[1].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[1].payload,
                         'projectId=project-id-2&pageToken=None')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    def test_organization_backup_scheduler_should_schedule_next_organization_scheduler_task_if_next_token_returned(
        self, list_project_ids, _1):
        # given
        list_project_ids.return_value = (
            ['project-id-1', 'project-id-2'], 'next_token')
        # when
        OrganizationBackupScheduler().schedule_backup()

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')

        self.assertEqual(len(tasks), 3)
        self.assertEqual(tasks[2].url,
                         '/tasks/schedulebackup/organization?pageToken=next_token')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    def test_organization_backup_scheduler_should_schedule_from_next_token_if_passed(
        self, list_project_ids, _1):
        # given
        list_project_ids.return_value = (['project-id-1', 'project-id-2'], None)
        # when
        OrganizationBackupScheduler().schedule_backup(page_token='next_token')

        # then
        list_project_ids.assert_called_once_with(page_token='next_token')

        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')
        self.assertEqual(len(tasks), 2)

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch.object(Configuration, 'backup_settings_custom_project_list',
                  new_callable=PropertyMock)
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    def test_organization_backup_scheduler_should_schedule_only_custom_project_list_if_defined(
        self, list_project_ids, custom_project_list, _1):
        # given
        custom_project_list.return_value = ['custom-id-1', 'custom-id-2']
        # when
        OrganizationBackupScheduler().schedule_backup()

        # then
        list_project_ids.assert_not_called()

        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[0].payload,
                         'projectId=custom-id-1&pageToken=None')
        self.assertEqual(tasks[1].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[1].payload,
                         'projectId=custom-id-2&pageToken=None')

    @patch.object(request_correlation_id, 'get', return_value='correlation-id')
    @patch.object(Configuration, 'projects_to_skip', new_callable=PropertyMock)
    @patch('src.commons.big_query.big_query.BigQuery.list_project_ids')
    def test_organization_backup_scheduler_should_schedule_only_not_skipped_projects(
        self, list_project_ids, projects_to_skip, _1):
        # given
        list_project_ids.return_value = (
            ['project-id-1', 'project-to-to-skip-2'], None)
        projects_to_skip.return_value = ['project-to-to-skip-2']
        # when
        OrganizationBackupScheduler().schedule_backup()

        # then
        tasks = self.task_queue_stub.get_filtered_tasks(
            queue_names='backup-scheduler')

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].url, '/tasks/schedulebackup/project')
        self.assertEqual(tasks[0].payload,
                         'projectId=project-id-1&pageToken=None')
