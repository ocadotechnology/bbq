import os

from src.backup.scheduler.organization import \
    organization_backup_scheduler_handler
from src.backup.scheduler.organization.organization_backup_scheduler import \
    OrganizationBackupScheduler
from src.commons.big_query.big_query import BigQuery

os.environ['SERVER_SOFTWARE'] = 'Development/'
import unittest

import webtest
from google.appengine.ext import testbed
from mock import patch


class TestOrganizationBackupSchedulerHandler(unittest.TestCase):
    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        self._create_http = patch.object(BigQuery, '_create_http').start()
        app = organization_backup_scheduler_handler.app
        self.under_test = webtest.TestApp(app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()
        patch.stopall()

    @patch.object(OrganizationBackupScheduler, "schedule_backup")
    def test_that_organization_backup_scheduler_parse_arguments_correctly(self,
        scheduler):
        # when
        self.under_test.get(url='/tasks/schedulebackup/organization?pageToken=somevalue')

        # then
        scheduler.called_only_once_with(page_token="somevalue")

    @patch.object(OrganizationBackupScheduler, "schedule_backup")
    def test_that_organization_backup_scheduler_parse_arguments_correctly(self,
        scheduler):
        # when
        self.under_test.get(url='cron_backup')
        # then
        scheduler.called_only_once_with()
