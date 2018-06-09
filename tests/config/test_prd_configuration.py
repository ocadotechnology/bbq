import unittest

from src.configuration import Configuration


######################
# YACHT private file #
######################
class TestPrdConfiguration(unittest.TestCase):

    prd_configuration = Configuration("config/prd/config.yaml")

    def test_should_be_able_to_read__copy_job_result_check_countdown_in_sec(self):
        self.assertEqual(self.prd_configuration.copy_job_result_check_countdown_in_sec, 60)

    def test_should_be_able_to_read__backup_worker_max_countdown_in_sec(self):
        self.assertEqual(self.prd_configuration.backup_worker_max_countdown_in_sec, 25200)

    def test_should_be_able_to_read__backup_project_id(self):
        self.assertEqual(self.prd_configuration.backup_project_id, '<your-project-id>')

    def test_should_be_able_to_read__restoration_project_id(self):
        self.assertEqual(self.prd_configuration.restoration_project_id, '<your-project-id>')

    def test_should_be_able_to_read__projects_to_skip(self):
        self.assertEqual(self.prd_configuration.projects_to_skip,
                         ['<your-project-id>'])

    def test_should_be_able_to_read__custom_project_list(self):
        self.assertEqual(self.prd_configuration.backup_settings_custom_project_list, [])
