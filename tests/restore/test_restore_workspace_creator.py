from unittest import TestCase

from google.appengine.ext import testbed, ndb
from mock import patch, ANY

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.restore.restore_workspace_creator import RestoreWorkspaceCreator
from src.commons.table_reference import TableReference

SOURCE_PROJECT_ID = "source_project_id_1"
SOURCE_DATASET_ID = "source_dataset_id_1"
SOURCE_TABLE_ID = "source_table_id_1"
SOURCE_PARTITION_ID = "source_partition_id_1"

TARGET_PROJECT_ID = "target_project_id_1"
TARGET_DATASET_ID = "target_dataset_id_1"
TARGET_TABLE_ID = "target_table_id_1"
TARGET_PARTITION_ID = "target_partition_id_1"


class TestRestoreWorkspaceCreator(TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_app_identity_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()
        self.BQ = patch(
            'src.restore.async_batch_restore_service.BigQuery').start()
        patch.object(BigQueryTableMetadata, "get_table_by_reference_cached").start()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def test_should_create_dataset_if_not_exist(self):
        # given
        source, target = self.__create_partitioned_table_references()
        self.BQ.get_dataset_cached.return_value = None

        # when
        RestoreWorkspaceCreator(self.BQ).create_workspace(source, target)

        # then
        self.BQ.create_dataset.assert_called_once()

    def test_should_create_dataset_based_on_target_table_reference(self):
        # given
        source, target = self.__create_partitioned_table_references()
        self.BQ.get_dataset_cached.return_value = None

        # when
        RestoreWorkspaceCreator(self.BQ).create_workspace(source, target)

        # then
        self.BQ.create_dataset.assert_called_with(project_id=target.project_id,
                                                  dataset_id=target.dataset_id,
                                                  location=ANY)

    def test_should_create_dataset_with_location_of_source_table(self):
        # given
        source, target = self.__create_partitioned_table_references()
        self.BQ.get_dataset_cached.return_value = None
        self.BQ.get_dataset_location.return_value = 'UK'

        # when
        RestoreWorkspaceCreator(self.BQ).create_workspace(source, target)

        # then
        self.BQ.get_dataset_location.assert_called_with(
            project_id=source.project_id,
            dataset_id=source.dataset_id
        )
        self.BQ.create_dataset.assert_called_with(
            project_id=ANY,
            dataset_id=ANY,
            location='UK'
        )

    def test_should_not_create_dataset_if_already_exist(self):
        # given
        enforcer = RestoreWorkspaceCreator(self.BQ)
        source, target = self.__create_partitioned_table_references()
        self.BQ.get_dataset_cached.return_value = "<DATASET METADATA RETURNED>"

        # when
        enforcer.create_workspace(source, target)

        # then
        self.BQ.create_dataset.assert_not_called()

    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=False)
    @patch.object(BigQueryTableMetadata, 'create_the_same_empty_table')
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference_cached', return_value=BigQueryTableMetadata({}))
    def test_should_create_table_if_is_partitioned_and_not_exist(self, _, create_the_same_empty_table, _1):
        # given
        enforcer = RestoreWorkspaceCreator(self.BQ)
        source, target = self.__create_partitioned_table_references()

        # when
        enforcer.create_workspace(source, target)

        # then
        create_the_same_empty_table.assert_called_with(target)

    @patch.object(BigQueryTableMetadata, 'create_the_same_empty_table')
    @patch.object(BigQueryTableMetadata, 'get_table_by_reference_cached')
    def test_should_not_create_table_if_is_not_partitioned(self, _, create_the_same_empty_table):
        # given
        enforcer = RestoreWorkspaceCreator(self.BQ)
        source, target = self.__create_non_partitioned_table_references()

        # when
        enforcer.create_workspace(source, target)

        # then
        create_the_same_empty_table.assert_not_called()

    @patch.object(BigQueryTableMetadata, 'get_table_by_reference_cached', return_value=BigQueryTableMetadata(None))
    @patch.object(BigQueryTableMetadata, 'table_exists', return_value=True)
    def test_should_not_create_table_if_table_already_exist(self, _, _1):
        # given
        enforcer = RestoreWorkspaceCreator(self.BQ)
        source, target = self.__create_partitioned_table_references()

        # when
        enforcer.create_workspace(source, target)

        # then
        self.BQ.create_empty_partitioned_table.assert_not_called()

    @patch.object(BigQueryTableMetadata, 'get_table_by_reference_cached')
    def test_create_workspace_should_take_care_about_dataset_and_table(self, _):
        # given
        enforcer = RestoreWorkspaceCreator(self.BQ)
        source, target = self.__create_partitioned_table_references()
        self.BQ.get_dataset_cached.return_value = "<DATASET METADATA RETURNED>"

        # when
        enforcer.create_workspace(source, target)

        # then
        self.BQ.get_dataset_cached.assert_called_once()
        BigQueryTableMetadata.get_table_by_reference_cached.assert_called_once()

    def __create_partitioned_table_references(self):
        source_table_reference = TableReference(SOURCE_PROJECT_ID,
                                                SOURCE_DATASET_ID,
                                                SOURCE_TABLE_ID,
                                                SOURCE_PARTITION_ID)
        target_table_reference = TableReference(TARGET_PROJECT_ID,
                                                TARGET_DATASET_ID,
                                                TARGET_TABLE_ID,
                                                TARGET_PARTITION_ID)
        return source_table_reference, target_table_reference

    def __create_non_partitioned_table_references(self):
        source_table_reference = TableReference(SOURCE_PROJECT_ID,
                                                SOURCE_DATASET_ID,
                                                SOURCE_TABLE_ID,
                                                None)
        target_table_reference = TableReference(TARGET_PROJECT_ID,
                                                TARGET_DATASET_ID,
                                                TARGET_TABLE_ID,
                                                None)
        return source_table_reference, target_table_reference
