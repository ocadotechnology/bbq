import unittest
from datetime import datetime, timedelta
import random

import logging
from mock import patch, call

from src.big_query.big_query import BigQuery, RandomizationError
from src.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.restore.test.table_randomizer import TableRandomizer, \
    DoesNotMeetSampleCriteriaException
from src.table_reference import TableReference


class TestTableRandomizer(unittest.TestCase):

    def setUp(self):
        patch('googleapiclient.discovery.build').start()
        patch(
            'src.configuration.Configuration.backup_project_id',
            return_value='project-id'
        ).start()
        patch('oauth2client.client.GoogleCredentials.get_application_default')\
            .start()

    def tearDown(self):
        patch.stopall()

    @patch.object(BigQuery, 'get_table', return_value={})
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQuery, 'fetch_random_table')
    def test_should_get_table_with_randomly_chosen_table(
            self, fetch_random_table, get_last_modified_datetime, get_table):
        # given
        sample_table = TableReference("w-d-40",
                                      "huge_dataset",
                                      "big_table_20170315")
        fetch_random_table.return_value = sample_table
        yesterday = datetime.utcnow() - timedelta(days=1)
        get_last_modified_datetime.return_value = yesterday
        under_test = TableRandomizer()

        # when
        table_metadata = under_test.get_random_table_metadata().table_metadata

        # then
        get_table.assert_called_with('w-d-40',
                                     'huge_dataset',
                                     'big_table_20170315',
                                     log_table=False)
        self.assertTrue(isinstance(table_metadata, dict))



    @patch.object(BigQuery,'fetch_random_table')
    @patch.object(BigQuery,'get_table')
    @patch.object(BigQueryTableMetadata, 'is_external_or_view_type', return_value=False)
    @patch.object(BigQueryTableMetadata, 'is_empty', return_value=False)
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQueryTableMetadata, 'is_daily_partitioned', return_value=True)
    @patch.object(BigQuery, 'list_table_partitions')
    @patch.object(BigQuery, 'get_table_or_partition')
    @patch.object(random, 'randint', return_value=1)
    def test_return_random_partition_when_table_is_partitioned(
            self, _, get_table_or_partition, list_table_partitions, _1,
            get_last_modified_datetime, _2, _3, get_table, fetch_random_table):
        # given
        get_table.return_value= {
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            },
            "timePartitioning": {
                "type": "DAY"
            },
            "creationTime": "1476794300804",
            "lastModifiedTime": "1481741498588",
            "type": "TABLE",
        }
        list_table_partitions.return_value = [
            {'partitionId': "20170908", 'creationTime': 0,
             'lastModifiedTime': 0},
            {'partitionId': "20170909", 'creationTime': 0,
             'lastModifiedTime': 0}
        ]
        fetch_random_table.return_value = TableReference("p1", "d1", "t1")
        yesterday = datetime.utcnow() - timedelta(days=1)
        get_last_modified_datetime.return_value = yesterday
        under_test = TableRandomizer()
        #
        # # when
        under_test.get_random_table_metadata()
        #
        # # then
        get_table_or_partition.assert_called_with('p1', 'd1', 't1', "20170909")

    @patch.object(BigQuery, 'get_table', return_value={})
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQuery, 'fetch_random_table')
    def test_should_raise_exception_and_retry_10_times_when_chosen_table_has_been_modified_today(  # nopep8 pylint: disable=C0301
            self, fetch_random_table, get_last_modified_datetime, get_table):  # nopep8 pylint: disable=W0613

        # given
        sample_table = TableReference("w-d-40", "huge_dataset", "big_table_20170315")

        fetch_random_table.return_value = sample_table
        get_last_modified_datetime.return_value = datetime.utcnow()
        under_test = TableRandomizer()

        # when then
        self.assertRaises(DoesNotMeetSampleCriteriaException,
                          under_test.get_random_table_metadata)
        self.assertEquals(10, fetch_random_table.call_count)

    @patch.object(BigQuery, 'get_table', return_value={})
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQueryTableMetadata, 'is_external_or_view_type')
    @patch.object(BigQuery, 'fetch_random_table')
    def test_should_raise_exception_when_table_is_view_or_external(self, fetch_random_table, is_external_or_view_type, get_last_modified_datetime, get_table):  # nopep8 pylint: disable=W0613,C0301

        # given
        sample_table = TableReference("w-d-40", "huge_dataset",
                                      "big_table_20170315")

        fetch_random_table.return_value = sample_table
        yesterday = datetime.utcnow() - timedelta(days=1)
        get_last_modified_datetime.return_value = yesterday
        is_external_or_view_type.return_value = True
        under_test = TableRandomizer()

        # when then
        self.assertRaises(DoesNotMeetSampleCriteriaException,
                          under_test.get_random_table_metadata)

    @patch.object(BigQuery, 'get_table', return_value={})
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQuery, 'fetch_random_table')
    def test_should_retry_randomization_on_RandomizationError_from_random_table_query(  # nopep8 pylint: disable=C0301
            self, fetch_random_table, get_last_modified_datetime, get_table):       # nopep8 pylint: disable=W0613

        # given
        sample_table = TableReference("w-d-40", "dataset", "table_20170315")

        fetch_random_table.side_effect = [RandomizationError('Boooom!'),
                                          sample_table]
        yesterday = datetime.utcnow() - timedelta(days=1)
        get_last_modified_datetime.return_value = yesterday

        # when
        under_test = TableRandomizer()
        under_test.get_random_table_metadata()

        # then
        self.assertEquals(2, fetch_random_table.call_count)

    @patch.object(BigQuery, 'get_table', return_value={})
    @patch.object(BigQueryTableMetadata, 'get_last_modified_datetime')
    @patch.object(BigQuery, 'fetch_random_table')
    @patch.object(logging, 'error')
    def test_that_if_table_was_created_yesterday_it_should_be_logged(
            self, error, fetch_random_table,
            get_last_modified_datetime, _):

        # given
        sample_table = TableReference("w-d-40", "dataset", "table_20170315")

        fetch_random_table.return_value = sample_table

        yesterday = datetime.utcnow() - timedelta(days=1)
        get_last_modified_datetime.return_value = yesterday

        # when
        under_test = TableRandomizer()
        under_test.get_random_table_metadata()

        # then
        error.assert_has_calls([call(
            'Table was modified yesterday. It is possible that last backup '
            'cycle did not cover it. Therefore restoration of this table '
            'can fail.')])


def load(filename):
    with open(filename, 'r') as f:
        return f.read()
