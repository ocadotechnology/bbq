import unittest

from apiclient.errors import HttpError
from apiclient.http import HttpMockSequence
from google.appengine.ext import testbed
from mock import patch

from src.commons.big_query.big_query import BigQuery, RandomizationError
from tests.test_utils import content


class TestBigQuery(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_app_identity_stub()
        self._create_http = patch.object(BigQuery, '_create_http').start()

    def tearDown(self):
        patch.stopall()
        self.testbed.deactivate()

    def test_iterating_projects(self):
        # given
        self._create_http.return_value = self.__create_project_list_responses()

        # when
        project_ids = BigQuery().list_project_ids()

        # then
        self.assertEqual(self.count(project_ids), 4)

    def test_iterating_datasets(self):
        # given
        self._create_http.return_value = self.__create_dataset_list_responses()

        # when
        dataset_ids = BigQuery().list_dataset_ids("project123")

        # then
        self.assertEqual(self.count(dataset_ids), 3)

    def test_get_table_should_ignore_400_error(self):
        # given
        self._create_http.return_value = self.__create_get_table_400_responses()

        # when
        table = BigQuery().get_table("project_id", "dataset_id", "table_id")

        # then
        self.assertIsNone(table)

    def test_iterating_tables(self):
        # given
        self._create_http.return_value = self.__create_tables_list_responses()

        # when
        tables_ids = BigQuery().list_table_ids("project1233", "dataset_id")

        # then
        self.assertEqual(self.count(tables_ids), 5)

    class TestClass(object):
        def func_for_test(self, project_id, dataset_id, table_id):
            pass

    @patch('time.sleep', return_value=None)
    @patch.object(TestClass, "func_for_test")
    def test_iterating_tables_should_retry_if_gets_http_503_response_once(self, func,_):
        # given
        self._create_http.return_value = self.__create_tables_list_responses_with_503()

        # when
        BigQuery().for_each_table("project1233", "dataset_id", func)

        # then
        self.assertEquals(5, func.call_count)

    def test_when_dataset_not_exist_then_iterating_tables_should_not_return_any_table(self):
        # given
        self._create_http.return_value = self.__create_dataset_not_found_during_tables_list_responses()

        # when
        tables_ids = BigQuery().list_table_ids("projectid", "dataset_id")

        # then
        self.assertEqual(self.count(tables_ids), 0)

    def test_listing_table_partitions(self):
        # given
        self._create_http.return_value = self.__create_table_partititions_list_responses()
        # when
        partitions = BigQuery() \
            .list_table_partitions("project123", "dataset123", "table123")

        # then
        self.assertEqual(self.count(partitions), 5)
        self.assertEqual(partitions[0]['partitionId'], '20170317')
        self.assertEqual(partitions[0]['creationTime'],
                         '2017-03-17 14:32:17.755000')
        self.assertEqual(partitions[0]['lastModifiedTime'],
                         '2017-03-17 14:32:19.289000')

    def test_should_fetch_single_result_from_random_table_query(self):
        # given
        self._create_http.return_value = self.__create_random_table_responses()
        # when
        random_table = BigQuery().fetch_random_table()

        # then
        self.assertEqual('project-dev', random_table.get_project_id())
        self.assertEqual('test_set', random_table.get_dataset_id())
        self.assertEqual('O_PRODUCT_SUPPLIER_20151127',
                         random_table.get_table_id())


    def test_get_dataset_cached_should_only_call_bq_once_but_response_is_cached(
            self):
        # given
        self._create_http.return_value = \
            self.__create_dataset_responses_with_only_one_response_for_get_dataset()
        # when
        bq = BigQuery()
        result1 = bq.get_dataset_cached('project', 'dataset')
        result2 = bq.get_dataset_cached('project', 'dataset')

        # then
        self.assertEqual(result1, result2)

    def test_should_raise_exception_if_random_table_query_returns_no_results(
            self):
        # given
        self._create_http.return_value = self.__create_random_table_no_results_responses()
        # when then
        self.assertRaises(RandomizationError, BigQuery().fetch_random_table)

    def test_create_dataset_happy_path(self):
        # given
        self._create_http.return_value = self.__create_dataset_create_responses()
        # when then
        BigQuery().create_dataset("project123", "dataset_id", "US")

    def test_create_dataset_do_nothing_if_dataset_already_exists(self):
        # given
        self._create_http.return_value = self.__create_dataset_create_already_exist_responses()
        # when then
        BigQuery().create_dataset("project123", "dataset_id", "US")

    @staticmethod
    def count(generator):
        return sum(1 for _ in generator)

    @staticmethod
    def __create_project_list_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_project_list_page_1.json')),
            ({'status': '200'}, content(
                'tests/json_samples/bigquery_project_list_page_last.json'))
        ])

    @staticmethod
    def __create_dataset_list_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_list_page_last.json'))
        ])

    @staticmethod
    def __create_get_table_400_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '400'},
             content(
                 'tests/json_samples/table_get/bigquery_view_get_400_read_partition.json'))
        ])

    @staticmethod
    def __create_tables_list_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_last.json'))
        ])

    @staticmethod
    def __create_tables_list_responses_with_503():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '503'},
             content('tests/json_samples/bigquery_table_list_503_error.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_1.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_table_list_page_last.json'))
        ])

    @staticmethod
    def __create_dataset_not_found_during_tables_list_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/bigquery_table_list_404_dataset_not_exist.json'))
        ])

    @staticmethod
    def __create_table_partititions_list_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_query_for_partitions.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/bigquery_query_for_partitions_results_1.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/bigquery_query_for_partitions_results_last.json'))
        ])

    @staticmethod
    def __create_random_table_responses():
        return HttpMockSequence([(
            {'status': '200'},
            content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/random_table/biquery_query_for_random_table.json'))
        ])

    @staticmethod
    def __create_table_responses_with_only_one_response_for_get_table():
        return HttpMockSequence([(
            {'status': '200'},
            content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/random_table/biquery_query_for_random_table.json'))
        ])

    @staticmethod
    def __create_dataset_responses_with_only_one_response_for_get_dataset():
        return HttpMockSequence([(
            {'status': '200'},
            content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_create.json')), ])

    @staticmethod
    def __create_random_table_no_results_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content(
                 'tests/json_samples/random_table/big_query_for_random_table_no_results.json'))
        ])

    @staticmethod
    def __create_dataset_create_responses():
        return HttpMockSequence([(
            {'status': '200'},
            content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '200'},
             content('tests/json_samples/bigquery_dataset_create.json')), ])

    @staticmethod
    def __create_dataset_create_already_exist_responses():
        return HttpMockSequence([
            ({'status': '200'},
             content('tests/json_samples/bigquery_v2_test_schema.json')),
            ({'status': '409'},
             content('tests/json_samples/bigquery_dataset_create.json')), ])
