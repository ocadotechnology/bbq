import unittest

from src.configuration import Configuration


class TestPrdConfiguration(unittest.TestCase):

    configuration = Configuration("config/config.yaml")

    def test_should_be_able_to_read__copy_job_result_check_countdown_in_sec(self):
        self.assertEqual(self.configuration.copy_job_result_check_countdown_in_sec, 5)

    def test_should_be_able_to_read__backup_worker_max_countdown_in_sec(self):
        self.assertEqual(self.configuration.backup_worker_max_countdown_in_sec, 0)

    def test_should_be_able_to_read__backup_project_id(self):
        self.assertEqual(self.configuration.backup_project_id, '<your-project-id>')

    def test_should_be_able_to_read__restoration_project_id(self):
        self.assertEqual(self.configuration.restoration_project_id, '<your-project-id>')

    def test_should_be_able_to_read__projects_to_skip(self):
        self.assertEqual(self.configuration.projects_to_skip, ['<your-project-id>'])

    def test_should_be_able_to_read__custom_project_list(self):
        self.assertEqual(self.configuration.backup_settings_custom_project_list, [])
