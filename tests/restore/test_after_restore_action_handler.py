import json
import unittest
from datetime import datetime

import webtest
from freezegun import freeze_time
from google.appengine.ext import testbed

from src.commons.table_reference import TableReference
from src.restore import after_restore_action_handler
from src.restore.datastore.restore_item import RestoreItem
from tests.commons.big_query.copy_job_async.result_check.job_result_example \
    import JobResultExample


class TestAfterRestoreActionHandler(unittest.TestCase):

    def setUp(self):
        self.init_webtest()

    def init_webtest(self):
        self.under_test = webtest.TestApp(after_restore_action_handler.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

    def tearDown(self):
        self.testbed.deactivate()

    @freeze_time("2017-12-21")
    def test_should_update_restore_item_when_copy_job_status_is_done(self):
        # given
        restore_item_key = self.prepare_initial_restore_item()
        payload = json.dumps(
            {"data": {"restoreItemKey": restore_item_key.urlsafe()},
             "jobJson": JobResultExample.DONE})

        # when
        self.under_test.post('/callback/restore-finished/', params=payload)

        # then
        updated_restore_item = RestoreItem.get_by_key(restore_item_key)
        self.assertEqual(updated_restore_item.status, RestoreItem.STATUS_DONE)
        self.assertEqual(updated_restore_item.completed, datetime.now())

    @freeze_time("2017-12-21")
    def test_should_update_restore_item_when_copy_job_status_has_errors(self):
        # given
        restore_item_key = self.prepare_initial_restore_item()
        expected_error_message = 'Copy job finished with errors: invalid:Cannot read a table without a schema, backendError:Backend error'
        payload = json.dumps(
            {"data": {"restoreItemKey": restore_item_key.urlsafe()},
             "jobJson": JobResultExample.DONE_WITH_NOT_REPETITIVE_ERRORS})

        # when
        self.under_test.post('/callback/restore-finished/', params=payload)

        # then
        updated_restore_item = RestoreItem.get_by_key(restore_item_key)
        self.assertEqual(updated_restore_item.status, RestoreItem.STATUS_FAILED)
        self.assertEqual(updated_restore_item.completed, datetime.now())
        self.assertEqual(updated_restore_item.status_message,
                         expected_error_message)

    def test_should_fail_when_not_json_is_passed_as_payload(self):
        # given
        expected_error = '{"status": "failed", "message": "No JSON object could be decoded", "httpStatus": 400}'
        payload = "not a json string"

        # when
        response = self.under_test.post('/callback/restore-finished/',
                                        params=payload, expect_errors=True)
        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    def test_should_fail_when_data_is_missing_in_payload(self):
        # given
        expected_error = '{"status": "failed", "message": "JSON should have \\"data\\" element", "httpStatus": 400}'
        payload = json.dumps(
            {"jobJson": {"state": "DONE"}})

        # when
        response = self.under_test.post('/callback/restore-finished/',
                                        params=payload, expect_errors=True)
        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    def test_should_fail_when_restore_item_key_is_missing_in_data(self):
        # given
        expected_error = '{"status": "failed", "message": "JSON should have \\"restoreItemKey\\" element", "httpStatus": 400}'
        payload = json.dumps(
            {"data": {},
             "jobJson": {"state": "DONE"}})

        # when
        response = self.under_test.post('/callback/restore-finished/',
                                        params=payload, expect_errors=True)
        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    def test_should_fail_when_job_status_is_missing_in_payload(self):
        # given
        expected_error = '{"status": "failed", "message": "JSON should have \\"jobJson\\" element", "httpStatus": 400}'
        payload = json.dumps(
            {"data": {"restoreItemKey": "restoreItemUrlSafeKeyValue"}})

        # when
        response = self.under_test.post('/callback/restore-finished/',
                                        params=payload, expect_errors=True)
        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    def prepare_initial_restore_item(self):
        restore_item = RestoreItem.create(
            TableReference('s_project', 's_dataset', 's_table', 's_partition'),
            TableReference('t_project', 't_dataset', 't_table', 't_partition'))
        restore_item.status = RestoreItem.STATUS_IN_PROGRESS
        return restore_item.put()
