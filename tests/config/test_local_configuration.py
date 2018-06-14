import unittest

from src.configuration import Configuration


######################
# YACHT private file #
######################
class TestDevConfiguration(unittest.TestCase):

    dev_configuration = Configuration("config/local/config.yaml")

    def test_should_be_able_to_read__copy_job_result_check_countdown_in_sec(self):
        self.assertEqual(self.dev_configuration.copy_job_result_check_countdown_in_sec, 5)

    def test_should_be_able_to_read__backup_worker_max_countdown_in_sec(self):
        self.assertEqual(self.dev_configuration.backup_worker_max_countdown_in_sec, 0)

    def test_should_be_able_to_read__backup_project_id(self):
        self.assertEqual(self.dev_configuration.backup_project_id, '<your-project-id>')

    def test_should_be_able_to_read__restoration_project_id(self):
        self.assertEqual(self.dev_configuration.restoration_project_id, '<your-project-id>')

    def test_should_be_able_to_read__projects_to_skip(self):
        self.assertEqual(self.dev_configuration.projects_to_skip, ['<your-project-id>'])

    def test_should_be_able_to_read__custom_project_list(self):
        self.assertEqual(self.dev_configuration.backup_settings_custom_project_list, [])

    def test_should_be_able_to_read__authorized_requestor_service_accounts(self):
        self.assertEqual(self.dev_configuration.authorized_requestor_service_accounts, ['sa-eligible-to-read-data@your-project-id.iam.gserviceaccount.com'])

    def test_should_be_able_to_read__restoration_daily_test_results_gcs_bucket(self):
        self.assertEqual(self.dev_configuration.restoration_daily_test_results_gcs_bucket, 'a-gcs-bucket')

    def test_should_be_able_to_read__restoration_daily_test_random_table_view(self):
        self.assertEqual(self.dev_configuration.restoration_daily_test_random_table_view, 'project:dataset.table')

