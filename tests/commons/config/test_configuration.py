import unittest

from src.commons.config.configuration import Configuration


class TestConfiguration(unittest.TestCase):

    configuration = Configuration("config/config.yaml")

    def test_should_be_able_to_read__copy_job_result_check_countdown_in_sec(self):
        self.__instance_of(self.configuration.copy_job_result_check_countdown_in_sec, int)

    def test_should_be_able_to_read__backup_project_id(self):
        self.__instance_of(self.configuration.backup_project_id, str)

    def test_should_be_able_to_read__metadata_storage_project_id(self):
        self.__instance_of(self.configuration.metadata_storage_project_id, str)

    def test_should_be_able_to_read__default_restoration_project_id(self):
        self.__instance_of(self.configuration.default_restoration_project_id, str)

    def test_should_be_able_to_read__projects_to_skip(self):
        self.__is_list_and_each_item_instance_of(self.configuration.projects_to_skip, str)

    def test_should_be_able_to_read__custom_project_list(self):
        self.__is_list_and_each_item_instance_of(self.configuration.projects_to_skip, str)

    def test_should_be_able_to_read_grace_period_after_source_table_deletion_in_months(self):
        self.__instance_of(self.configuration.grace_period_after_source_table_deletion_in_months, int)

    def test_should_be_able_to_read_young_old_generation_threshold_in_months(self):
        self.__instance_of(self.configuration.young_old_generation_threshold_in_months, int)

    def __instance_of(self, obj, expected_type):
        self.assertTrue(isinstance(obj, expected_type))

    def __is_list_and_each_item_instance_of(self, obj, expected_type):
        self.__instance_of(obj, list)
        for item in obj:
            self.__instance_of(item, expected_type)
