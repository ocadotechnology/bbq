import unittest

from src.commons.handlers import validators
from src.commons.handlers.validators import WrongDatasetNameException, \
    WrongProjectNameException, WrongCreateDispositionException, \
    WrongWriteDispositionException


class TestValidators(unittest.TestCase):

    def test_validate_restore_request_params_happy_path(self):
        validators.validate_restore_request_params(
            source_project_id="source-project-id",
            source_dataset_id="source_dataset_id",
            target_project_id="target-project-id",
            target_dataset_id="target_dataset_id",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"
        )

    def test_validate_restore_request_params_for_none_parameters(self):
        validators.validate_restore_request_params(
            source_project_id=None,
            source_dataset_id=None,
            target_project_id=None,
            target_dataset_id=None,
            create_disposition=None,
            write_disposition=None
        )

    # --------------- PROJECT ID TESTS -------------------
    def test_valid_project_id(self):
        validators.validate_project_id('project-id')

    def test_invalid_project_with_underscore(self):
        with self.assertRaises(WrongProjectNameException):
            validators.validate_project_id('project_123')

    def test_invalid_none_project(self):
        with self.assertRaises(WrongProjectNameException):
            validators.validate_project_id(None)

    def test_invalid_empty_project(self):
        with self.assertRaises(WrongProjectNameException):
            validators.validate_project_id('')

    def test_invalid_empty_project_with_spaces(self):
        with self.assertRaises(WrongProjectNameException):
            validators.validate_project_id('    ')

    def test_invalid_project_with_slash(self):
        with self.assertRaises(WrongProjectNameException):
            validators.validate_project_id('project/')

    # --------------- DATASET ID TESTS -------------------
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

    # ----------- CREATE DISPOSITION TESTS ---------------
    def test_valid_create_disposition(self):
        validators.validate_create_disposition('CREATE_IF_NEEDED')

    def test_invalid_create_disposition(self):
        with self.assertRaises(WrongCreateDispositionException):
            validators.validate_create_disposition('INVALID')

    def test_invalid_none_create_disposition(self):
        with self.assertRaises(WrongCreateDispositionException):
            validators.validate_create_disposition(None)

    def test_invalid_empty_create_disposition(self):
        with self.assertRaises(WrongCreateDispositionException):
            validators.validate_create_disposition('')

    def test_invalid_empty_create_disposition_with_spaces(self):
        with self.assertRaises(WrongCreateDispositionException):
            validators.validate_create_disposition('    ')

    # ----------- WRITE DISPOSITION TESTS ---------------
    def test_valid_write_disposition(self):
        validators.validate_write_disposition('WRITE_TRUNCATE')

    def test_invalid_write_disposition(self):
        with self.assertRaises(WrongWriteDispositionException):
            validators.validate_write_disposition('INVALID')

    def test_invalid_none_write_disposition(self):
        with self.assertRaises(WrongWriteDispositionException):
            validators.validate_write_disposition(None)

    def test_invalid_empty_write_disposition(self):
        with self.assertRaises(WrongWriteDispositionException):
            validators.validate_write_disposition('')

    def test_invalid_empty_write_disposition_with_spaces(self):
        with self.assertRaises(WrongWriteDispositionException):
            validators.validate_write_disposition('    ')
