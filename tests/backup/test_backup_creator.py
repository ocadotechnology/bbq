import datetime
import unittest

from freezegun import freeze_time
from mock import patch, PropertyMock

from src.commons.big_query.copy_job_async.copy_job_service_async \
    import CopyJobServiceAsync
from src.backup.datastore.Table import Table
# pylint: disable=W0613,R0201
from src.backup.backup_creator import BackupCreator
from src.commons.big_query.big_query_table import BigQueryTable
from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata


class TestBackupCreator(unittest.TestCase):


    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch(
            'oauth2client.client.GoogleCredentials.get_application_default')\
            .start()
        patch('src.commons.request_correlation_id.get',
              return_value='correlation-id').start()
        patch(
            'src.commons.config.configuration.Configuration.backup_project_id',
            new_callable=PropertyMock,
            return_value='bkup_storage_project'
        ).start()

    def tearDown(self):
        patch.stopall()

    @patch.object(CopyJobServiceAsync, 'copy_table')
    @freeze_time("2018-04-16")
    def test_that_async_copy_job_is_called_with_correct_parameters_when_creating_new_backup(     # pylint: disable=C0301
            self, async_copy):

        # given
        table_to_backup = Table(project_id="src_project",
                                dataset_id="src_dataset",
                                table_id="src_table",
                                partition_id="20180416")
        source_bq_table = BigQueryTable("src_project",
                                        "src_dataset",
                                        "src_table$20180416")
        destination_bq_table = BigQueryTable("bkup_storage_project",
                                             "2018_16_US_src_project",
                                             "20180416_000000_src_project_src_dataset_src_table_partition_20180416")   # pylint: disable=C0301
        under_test = BackupCreator(datetime.datetime.utcnow())

        # when
        under_test.create_backup(table_to_backup, BigQueryTableMetadata({}))

        # then
        async_copy.assert_called_with(source_bq_table, destination_bq_table)
