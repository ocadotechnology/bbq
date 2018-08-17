import unittest
from datetime import datetime

from src.backup.dataset_id_creator import DatasetIdCreator
from src.commons.exceptions import ParameterValidationException


class TestDatasetIdCreator(unittest.TestCase):
    def test_create_happy_path(self):
        # given
        date = datetime(1901, 12, 21)
        location = 'US'
        project = 'project123'

        # when
        result = DatasetIdCreator.create(date, location, project)

        # then
        expected_result = '1901_51_US_project123'

        self.assertEquals(result, expected_result)

    def test_create_should_replace_all_pauses_to_emphasis(self):
        # given
        date = datetime(1901, 01, 10)
        location = 'U-S'
        project = 'project-_123'

        # when
        result = DatasetIdCreator.create(date, location, project)

        # then
        expected_result = '1901_02_U_S_project__123'

        self.assertEquals(result, expected_result)

    def test_create_without_date_should_throw_error(self):
        # given
        date = None
        location = 'US'
        project = 'project_id'

        # when then
        self.assertRaises(ParameterValidationException,
                          DatasetIdCreator.create, date, location, project)

    def test_create_without_location_should_throw_error(self):
        # given
        date = datetime(2016, 5, 15)
        location = None
        project = 'project_id'

        # when then
        self.assertRaises(ParameterValidationException,
                          DatasetIdCreator.create, date, location, project)

    def test_create_without_project_should_throw_error(self):
        # given
        date = datetime(2016, 5, 15)
        location = 'US'
        project = None

        # when then
        self.assertRaises(ParameterValidationException,
                          DatasetIdCreator.create, date, location, project)
