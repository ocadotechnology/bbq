from datetime import datetime
from unittest import TestCase

from google.appengine.ext import testbed
from mock import patch

from src.commons.exceptions import ParameterValidationException
from src.restore.dataset.dataset_restore_parameters_validator import \
    DatasetRestoreParametersValidator
from tests.utils import table_entities_creator

PROJECT_TO_RESTORE = 'project-x'
DATASET_TO_RESTORE = 'dataset_y'

DATASET_WITH_US = {'location': 'US'}
DATASET_WITH_EU = {'location': 'EU'}


class TestDatasetRestoreParametersValidator(TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.BQ = patch(
            'src.restore.dataset.dataset_restore_parameters_validator.BigQuery').start()
        self.BQ.return_value = self.BQ

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def test_should_not_throw_exception_when_default_target_dataset_not_exist(
            self):
        # given
        self.BQ.get_dataset.return_value = None

        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())

        # when
        DatasetRestoreParametersValidator().validate_parameters(
            PROJECT_TO_RESTORE, DATASET_TO_RESTORE, None, None)

    def test_should_not_throw_exception_when_custom_target_dataset_not_exist(
            self):
        # given
        self.BQ.get_dataset.return_value = None

        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())

        # when
        DatasetRestoreParametersValidator().validate_parameters(
            PROJECT_TO_RESTORE, DATASET_TO_RESTORE, 'CUSTOM_TARGET_DATASET',
            None)

    def test_should_not_throw_exception_when_the_same_location(
            self):
        # given
        self.BQ.get_dataset.side_effect = [DATASET_WITH_US,
                                           DATASET_WITH_US]

        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())

        # when
        DatasetRestoreParametersValidator().validate_parameters(
            PROJECT_TO_RESTORE, DATASET_TO_RESTORE, 'CUSTOM_TARGET_DATASET',
            None)

    def test_should_throw_exception_when_default_dataset_has_different_location(
            self):
        # given
        self.BQ.get_dataset.side_effect = [DATASET_WITH_US, DATASET_WITH_EU]

        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())

        # when
        with self.assertRaises(ParameterValidationException) as ex:
            DatasetRestoreParametersValidator().validate_parameters(
                PROJECT_TO_RESTORE, DATASET_TO_RESTORE, None,
                None)

        # then
        self.assertTrue(
            'Target dataset already exist and has different'
            ' location than backup dataset' in str(ex.exception))

    def test_should_throw_exception_when_custom_dataset_has_different_location(
            self):
        # given
        self.BQ.get_dataset.side_effect = [DATASET_WITH_US, DATASET_WITH_EU]

        table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())

        # when
        with self.assertRaises(ParameterValidationException) as ex:
            DatasetRestoreParametersValidator().validate_parameters(
                PROJECT_TO_RESTORE, DATASET_TO_RESTORE, 'CUSTOM_TARGET_DATASET',
                None)

        # then
        self.assertTrue(
            'Target dataset already exist and has different'
            ' location than backup dataset' in str(ex.exception))

    def test_should_throw_exception_when_no_tables_found_in_datastore(self):
        # when
        with self.assertRaises(ParameterValidationException) as ex:
            DatasetRestoreParametersValidator().validate_parameters(
                PROJECT_TO_RESTORE, DATASET_TO_RESTORE, None, None)

        # then
        self.assertTrue('No Tables was found in Datastore' in str(ex.exception))

    def test_should_throw_exception_when_no_backups_for_tables_found_in_datastore(
            self):
        # given
        table = table_entities_creator.create_and_insert_table_with_one_backup(
            project_id=PROJECT_TO_RESTORE, dataset_id=DATASET_TO_RESTORE,
            table_id='tbl_name', date=datetime.now())
        backup = table.last_backup
        backup.deleted = datetime.now()
        backup.put()
        # when
        with self.assertRaises(ParameterValidationException) as ex:
            DatasetRestoreParametersValidator().validate_parameters(
                PROJECT_TO_RESTORE, DATASET_TO_RESTORE, None, None)

        # then
        self.assertTrue(
            'No Backups was found in Datastore' in str(ex.exception))
