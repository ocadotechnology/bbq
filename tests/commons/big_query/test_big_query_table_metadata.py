import datetime
import unittest

from google.appengine.ext import testbed
# nopep8 pylint: disable=C0301
from mock import patch
from mock.mock import Mock

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.big_query.big_query import BigQuery
from src.commons.error_reporting import ErrorReporting


# table_not_exist_anymore() method tests
from src.commons.table_reference import TableReference

class TestBigQueryTableMetadata_CreateTheSameEmptyTable(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        # patch.stopall()
        self.testbed.deactivate()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQuery, 'create_table')
    def test_create_same_empty_table_execute_a_proper_big_query_request_with_same_table_properties(self, create_table):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

        table_properties = {
            "tableReference":{
                "projectId":"p1",
                "datasetId":"d1",
                "tableId":"t1",
            },
            "timePartitioning": {
                "type": "DAY",
                "field": "birth_date"
            },
            "schema": "a schema"
        }

        target_table_reference = TableReference("p2", "d2", "t2")
        BigQueryTableMetadata(table_properties).create_the_same_empty_table(target_table_reference)
        create_table.assert_called_with("p2","d2",
            {
            "tableReference":{
                "projectId":"p2",
                "datasetId":"d2",
                "tableId":"t2",
            },
            "timePartitioning": {
                "type": "DAY",
                "field": "birth_date"
            },
            "schema": "a schema"
        })


class TestBigQueryTableMetadata_GetTableByReferenceCached(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        # patch.stopall()
        self.testbed.deactivate()

    @patch('src.commons.big_query.big_query.BigQuery.__init__', Mock(return_value=None))
    @patch.object(BigQuery, 'get_table', return_value={})
    def test_get_table_cached_should_only_call_bq_once(self, get_table):
        # given

        # when
        result1 = BigQueryTableMetadata.get_table_by_reference_cached(TableReference('project', 'dataset', 'table'))
        result2 = BigQueryTableMetadata.get_table_by_reference_cached(TableReference('project', 'dataset', 'table'))

        # then
        self.assertEqual(result1, result2)
        get_table.assert_called_once()


class TestBigQueryTableMetadata_TableExists(unittest.TestCase):

    def test_should_return_true_if_json_is_None(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata(None)
        # when
        result = big_query_table_metadata.table_exists()
        # then
        self.assertFalse(result)

    def test_should_return_false_if_json_is_not_None(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.table_exists()
        # then
        self.assertTrue(result)


# is_external_or_view_type() method tests
class TestBigQueryTableMetadata_IsExternalOrViewType(unittest.TestCase):

    def test_should_return_true_if_EXTERNAL_type(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"type":"EXTERNAL"})
        # when
        result = big_query_table_metadata.is_external_or_view_type()
        # then
        self.assertTrue(result)

    def test_should_return_true_if_VIEW_type(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"type":"VIEW"})
        # when
        result = big_query_table_metadata.is_external_or_view_type()
        # then
        self.assertTrue(result)

    def test_should_return_false_if_TABLE_type(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"type":"TABLE"})
        # when
        result = big_query_table_metadata.is_external_or_view_type()
        # then
        self.assertFalse(result)

    def test_should_return_false_if_no_type_field(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_external_or_view_type()
        # then
        self.assertFalse(result)


# is_external_or_view_type() method tests
class TestBigQueryTableMetadata_IsEmpty(unittest.TestCase):

    def test_should_return_false_if_table_contains_some_rows(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"numRows":"1"})
        # when
        result = big_query_table_metadata.is_empty()
        # then
        self.assertFalse(result)

    def test_should_return_true_if_table_contains_some_rows(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"numRows":"0"})
        # when
        result = big_query_table_metadata.is_empty()
        # then
        self.assertTrue(result)


# get_last_modified_datetime() method tests
class TestBigQueryTableMetadata_GetLastModifiedDatetime(unittest.TestCase):
    def test_should_extract_lastModifiedTime_and_convert_epoch_to_datetime(self):
        # given
        big_query_table_metadata = \
            BigQueryTableMetadata({"lastModifiedTime":1479308286000})
        # when
        result = big_query_table_metadata.get_last_modified_datetime()
        # then
        self.assertEqual(datetime.datetime(2016, 11, 16, 14, 58, 6), result)


# get_creation_time() method tests
class TestBigQueryTableMetadata_GetCreationDatetime(unittest.TestCase):
    def test_should_extract_lastModifiedTime_and_convert_epoch_to_datetime(self):
        # given
        big_query_table_metadata = \
            BigQueryTableMetadata({"creationTime": 1479308286000})
        # when
        result = big_query_table_metadata.get_creation_time()
        # then
        self.assertEqual(datetime.datetime(2016, 11, 16, 14, 58, 6), result)


# table_size_in_bytes() method tests
class TestBigQueryTableMetadata_TableSizeInBytes(unittest.TestCase):

    def test_should_extract_table_size_in_bytes(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"numBytes":123})
        # when
        result = big_query_table_metadata.table_size_in_bytes()
        # then
        self.assertEqual(123, result)


# table_numRows() method tests
class TestBigQueryTableMetadata_TableNumRows(unittest.TestCase):

    def test_should_extract_table_rows_count(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"numRows":12})
        # when
        result = big_query_table_metadata.table_rows_count()
        # then
        self.assertEqual(12, result)


# get_location() method tests
class TestBigQueryTableMetadata_GetLocation(unittest.TestCase):
    def test_should_extract_location_if_exist(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"location":"EU"})
        # when
        result = big_query_table_metadata.get_location()
        # then
        self.assertEqual("EU", result)

    def test_should_return_US_if_location_not_exist(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.get_location()
        # then
        self.assertEqual("US", result)


# is_localized_in_EU() method tests
class TestBigQueryTableMetadata_IsLocalizedInEU(unittest.TestCase):

    def test_should_return_TRUE_if_dataset_is_localized_in_EU(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"location": "EU"})
        # when
        result = big_query_table_metadata.is_localized_in_EU()
        # then
        self.assertTrue(result)

    def test_should_return_FALSE_if_dataset_is_localized_in_US(
            self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"location": "STH"})
        # when
        result = big_query_table_metadata.is_localized_in_EU()
        # then
        self.assertFalse(result)

    def test_should_return_FALSE_if_location_not_exist(
            self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_localized_in_EU()
        # then
        self.assertFalse(result)


# is_schema_defined() method tests
class TestBigQueryTableMetadata_IsSchemaDefined(unittest.TestCase):

    def test_should_return_True_if_schema_exists(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({"schema": {
            "fields": [
                {
                    "name": "Field_1",
                    "type": "STRING",
                    "mode": "NULLABLE"
                }
            ]
        }})
        # when
        result = big_query_table_metadata.is_schema_defined()
        # then
        self.assertTrue(result)

    def test_should_return_False_if_schema_doesnt_exist(
        self):
        # given
        # without schema field
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_schema_defined()
        # then
        self.assertFalse(result)

# is_daily_partitionded() method tests
class TestBigQueryTableMetadata_IsDailyPartitioned(unittest.TestCase):

    def setUp(self):
        patch(
            'src.commons.config.environment.Environment.version_id',
            return_value='dummy_version'
        ).start()
        patch(
            'src.commons.config.configuration.Configuration.backup_project_id',
            return_value='dummy_version'
        ).start()
        patch('googleapiclient.discovery.build').start()
        patch('oauth2client.client.GoogleCredentials.get_application_default')\
            .start()

    def tearDown(self):
        patch.stopall()

    def test_should_return_True_if_is_a_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata(
            {"tableReference":
                 {"tableId": "tableName$20170324"},
             "timePartitioning": {"type": "DAY"}
            }
        )
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(True, result)

    def test_should_return_False_if_there_is_no_partitioning_field(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({})
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)

    def test_should_return_True_if_there_is_DAY_type_in_timePartitioning_field(
            self
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference":{"tableId": "tableName"},
            "timePartitioning": {"type": "DAY"}
        })
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(True, result)

    @patch.object(ErrorReporting, '_create_http')
    @patch.object(ErrorReporting, 'report')
    def test_should_return_False_and_report_error_if_there_is_no_DAY_type_in_timePartitioning(self, report, _create_http):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {"tableId": "tableName"},
            "timePartitioning":"Something"
        })
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)
        report.assert_called_once()

    @patch.object(ErrorReporting, '_create_http')
    @patch.object(ErrorReporting, 'report')
    def test_should_return_False_and_report_error_if_there_is_other_type_of_timePartitioning(
            self, report, _create_http
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "timePartitioning": {"type": "OTHERTYPE"},
            "tableReference": {"tableId": "tableName"}
        })
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)
        report.assert_called_once()

    def test_should_return_False_if_no_metadata_for_table(
            self
    ):
        # given
        big_query_table_metadata = BigQueryTableMetadata(None)
        # when
        result = big_query_table_metadata.is_daily_partitioned()
        # then
        self.assertEqual(False, result)


# is_partition() method tests
class TestBigQueryTableMetadata_IsPartition(unittest.TestCase):

    def test_should_return_true_for_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.is_partition()
        # then
        self.assertEqual(True, result)

    def test_should_return_false_for_table(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when
        result = big_query_table_metadata.is_partition()
        # then
        self.assertEqual(False, result)


# get_partition_id() method tests
class TestBigQueryTableMetadata_GetPartitionId(unittest.TestCase):

    def test_should_return_partition_id(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.get_partition_id()
        # then
        self.assertEqual("20171002", result)

    def test_should_raise_exception_for_tables(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when then
        with self.assertRaises(AssertionError):
            big_query_table_metadata.get_partition_id()


# get_table_id() method tests
class TestBigQueryTableMetadata_GetTableId(unittest.TestCase):

    def test_should_return_table_id_when_partition(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.get_table_id()
        # then
        self.assertEqual("t1", result)

    def test_should_return_table_id_when_not_partitioned(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when
        result = big_query_table_metadata.get_table_id()
        # then
        self.assertEqual("t1", result)


# table_reference() method tests
class TestBigQueryTableMetadata_TableReference(unittest.TestCase):

    def test_should_return_partitioned_table_reference(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1$20171002"
            }
        })
        # when
        result = big_query_table_metadata.table_reference()
        # then
        self.assertEqual(TableReference("p1", "d1", "t1", "20171002"), result)

    def test_should_return_non_partitioned_table_reference(self):
        # given
        big_query_table_metadata = BigQueryTableMetadata({
            "tableReference": {
                "projectId": "p1",
                "datasetId": "d1",
                "tableId": "t1"
            }
        })
        # when
        result = big_query_table_metadata.table_reference()
        # then
        self.assertEqual(TableReference("p1", "d1", "t1"), result)

