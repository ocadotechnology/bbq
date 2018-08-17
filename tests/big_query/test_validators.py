import unittest

from src.commons.big_query import validators
from src.commons.big_query.validators import WrongDatasetNameException


class TestValidators(unittest.TestCase):

    def test_valid_dataset_id(self):
        validators.validate_dataset_id('DATA_set_123')

    def test_invalid_dataset_with_dash(self):
        with self.assertRaises(WrongDatasetNameException):
            validators.validate_dataset_id('dataset-123')

    def test_invalid_none_dataset(self):
        with self.assertRaises(WrongDatasetNameException):
            validators.validate_dataset_id(None)

    def test_invalid_empty_dataset(self):
        with self.assertRaises(WrongDatasetNameException):
            validators.validate_dataset_id('')

    def test_invalid_empty_dataset_with_spaces(self):
        with self.assertRaises(WrongDatasetNameException):
            validators.validate_dataset_id('    ')

    def test_invalid_dataset_with_slash(self):
        with self.assertRaises(WrongDatasetNameException):
            validators.validate_dataset_id('data/')

