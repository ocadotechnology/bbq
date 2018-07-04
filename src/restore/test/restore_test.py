import json
import logging
from datetime import datetime

from src.big_query.big_query import BigQuery
from src.configuration import configuration
from src.google_cloud_storage_client import GoogleCloudStorageClient as gcs
from src.restore.test.table_randomizer import TableRandomizer
from src.restore.test.table_restore_invoker import TableRestoreInvoker
from src.table_reference import TableReference

RESTORE_DATASET_ID_US = 'smoke_test_US'
RESTORE_DATASET_ID_EU = 'smoke_test_EU'


class RestoreTest(object):

    def test(self, host_url):
        src_table = self.__pick_random_table()
        table_reference = src_table.table_reference()
        table_restore_invoker = TableRestoreInvoker(host_url)

        restoration_job_id = table_restore_invoker.invoke(
            src_table_reference=table_reference,
            target_dataset=self.__get_restore_dataset(src_table),
            restoration_point_date=datetime.utcnow().date()
        )

        response = table_restore_invoker.wait_till_done(restoration_job_id)
        table_has_been_restored = response["status"]["result"] == "Success"

        if table_has_been_restored:
            self.__check_restored_table_matches_source(response, src_table)
        else:
            resp_msg = "Restore test failed. " \
                       "Failed to restore a table {}".format(table_reference)
            self.__save_test_status(table_reference, False, response)
            raise Exception(resp_msg)

    @staticmethod
    def __get_restore_dataset(src_table):
        return RESTORE_DATASET_ID_EU if src_table.is_localized_in_EU() \
            else RESTORE_DATASET_ID_US

    def __check_restored_table_matches_source(self, response, src_table):
        restoration_items = response["restorationItems"]
        assert len(restoration_items) == 1

        target_table_reference = \
            TableReference.parse_tab_ref(restoration_items[0]['targetTable'])
        target_table = BigQuery().get_table_by_reference(target_table_reference)

        tables_match, assertion_msg = \
            self.__assert_restored_table_matches_source(src_table, target_table)
        source_table_reference = src_table.table_reference()
        if tables_match:
            resp_msg = "Restore test completed successfully for table {}" \
                .format(source_table_reference)
            self.__save_test_status(source_table_reference, True, response)
            return resp_msg
        else:
            resp_msg = "Restore test failed. " \
                       "Table was restored, but it differs from source " \
                       "table. {0} {1}".format(assertion_msg, response)
            self.__save_test_status(source_table_reference, False, response)
            raise Exception(resp_msg)

    @staticmethod
    def __pick_random_table():
        src_table = TableRandomizer().get_random_table_metadata()
        logging.info("Randomly selected table: %s",
                     json.dumps(src_table.table_metadata))
        return src_table

    def __save_test_status(self, src_tbl_reference, test_passed,
                           restore_response):
        try:
            self.__save_status_file(src_tbl_reference,
                                    test_passed,
                                    restore_response)
        except Exception as e:
            resp_msg = "Unable to write JSON status file to GCS: " \
                       + e.message
            logging.exception(resp_msg)
            raise e

    @staticmethod
    def __assert_restored_table_matches_source(source_table, target_table):
        if source_table.table_rows_count() != target_table.table_rows_count():
            msg = 'Restored table has different no of rows {0} vs {1}'.format(
                source_table.table_rows_count(),
                target_table.table_rows_count())
            return False, msg
        if source_table.table_size_in_bytes() != \
                target_table.table_size_in_bytes():
            msg = "Restored table has different size {0} vs {1}".format(
                source_table.table_size_in_bytes(),
                target_table.table_size_in_bytes())
            return False, msg
        return True, ""

    @staticmethod
    def __save_status_file(table_reference, test_passed, restore_response):
        status = 'success' if test_passed else 'failed'
        status_file = {'status': status,
                       'tableReference': str(table_reference),
                       'restore_response': restore_response}
        gcs.put_gcs_file_content(
            configuration.restoration_daily_test_results_gcs_bucket,
            'restore-test-status.json',
            json.dumps(status_file),
            'application/json'
        )
