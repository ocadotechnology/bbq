import json
import os

from google.appengine.ext import ndb, testbed

from src.restore.list.backup_list_restore_service \
    import BackupListRestoreService, BackupItem


os.environ['SERVER_SOFTWARE'] = 'Development/'
from src.restore.list import backup_list_restore_handler

import unittest

import webtest
from mock import patch


class TestBackupListRestoreHandler(unittest.TestCase):
    def setUp(self):
        self.under_test = webtest.TestApp(backup_list_restore_handler.app)
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_memcache_stub()
        self.testbed.init_datastore_v3_stub()

    @patch.object(BackupListRestoreService, 'restore', return_value="123")
    def test_default_parameters_provided_for_list_restoration(
            self, mocked_restore_service):
        # given
        key1 = ndb.Key("Backup1", None)
        key2 = ndb.Key("Backup2", None)

        request_body = "[{{\"backupUrlSafeKey\": \"{}\"," \
                       "\"test_param_key\":\"test_value\"}}," \
                       "{{\"backupUrlSafeKey\": \"{}\"}}]". \
            format(key1.urlsafe(), key2.urlsafe())

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json'
        )

        # then
        response_json = json.loads(response.body)
        self.assertTrue('restorationJobId' in response_json)

        mocked_restore_service_request = \
            self.__get_mocked_service_argument(mocked_restore_service)
        passed_backup_items = list(mocked_restore_service_request.backup_items)

        self.assertEqual(len(passed_backup_items), 2)

        expected_backup_item_1 = \
            BackupItem(key1, {"test_param_key": "test_value"})
        expected_backup_item_2 = BackupItem(key2, {})

        # no order required
        self.assertIn(expected_backup_item_1, passed_backup_items)
        self.assertIn(expected_backup_item_2, passed_backup_items)

    @patch.object(BackupListRestoreService, 'restore', return_value="123")
    def test_should_fail_on_wrong_dataset_format(self, mocked_restore_service):
        # given
        expected_error = '{"status": "failed", "message": "Invalid dataset ' \
                         'value: \'dataset-with-dash\'. Dataset IDs may ' \
                         'contain letters, numbers, and underscores", ' \
                         '"httpStatus": 400}'
        mocked_restore_service.return_value = {"restorationJobId": "restore_id"}

        # when
        request_body = "[{\"backupUrlSafeKey\": \"url_safe_key1\"," \
                       "\"test_param_key\":\"test_value\"}," \
                       "{\"backupUrlSafeKey\": \"url_safe_key2\"}]"
        response = self.under_test.post(
            url='/restore/list?targetDatasetId=dataset-with-dash',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(BackupListRestoreService, 'restore', return_value="123")
    def test_should_fail_on_wrong_write_disposition_format(self,
                                                           mocked_restore_service):
        # given
        expected_error = '{"status": "failed", "message": "Invalid write ' \
                         'disposition: \'WRONG_WRITE\'. The following values ' \
                         'are supported: WRITE_TRUNCATE, WRITE_APPEND, ' \
                         'WRITE_EMPTY.", "httpStatus": 400}'
        mocked_restore_service.return_value = {"restorationJobId": "restore_id"}

        # when
        request_body = "[{\"backupUrlSafeKey\": \"url_safe_key1\"," \
                       "\"test_param_key\":\"test_value\"}," \
                       "{\"backupUrlSafeKey\": \"url_safe_key2\"}]"
        response = self.under_test.post(
            url='/restore/list?writeDisposition=WRONG_WRITE',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(BackupListRestoreService, 'restore', return_value="123")
    def test_should_fail_on_wrong_create_disposition_format(self,
                                                            mocked_restore_service):
        # given
        expected_error = '{"status": "failed", "message": "Invalid create ' \
                         'disposition: \'WRONG_CREATE\'. The following values ' \
                         'are supported: CREATE_IF_NEEDED, CREATE_NEVER.", ' \
                         '"httpStatus": 400}'
        mocked_restore_service.return_value = {"restorationJobId": "restore_id"}

        # when
        request_body = "[{\"backupUrlSafeKey\": \"url_safe_key1\"," \
                       "\"test_param_key\":\"test_value\"}," \
                       "{\"backupUrlSafeKey\": \"url_safe_key2\"}]"
        response = self.under_test.post(
            url='/restore/list?createDisposition=WRONG_CREATE',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    def test_should_fail_with_status_400_if_request_body_is_not_a_valid_json(
            self):
        # given
        incorrect_request_bodies = ["{something}", "{'age':100 }", "<xml>"]

        # when
        for request_body in incorrect_request_bodies:
            response = self.under_test.post(
                url='/restore/list',
                params=request_body,
                content_type='application/json',
                expect_errors=True
            )
        # then
        self.assertEquals(400, response.status_int)

    def test_should_fail_with_status_400_if_request_body_has_duplicated_keys(
            self):
        # given
        key1 = ndb.Key("Backup", None)
        key2 = ndb.Key("Backup", None)
        request_body = "[{{\"backupUrlSafeKey\": \"{}\"," \
                       "\"test_param_key\":\"test_value\"}}," \
                       "{{\"backupUrlSafeKey\": \"{}\"}}]". \
            format(key1.urlsafe(), key2.urlsafe())

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )
        # then
        self.assertEquals(400, response.status_int)
        self.assertIn("There are duplicated backup items", response)

    def test_should_fail_with_status_400_if_has_duplicated_keys_in_two_formats(
            self):
        # given

        bq_key = "\"Table\", 6394035673497600, " \
                 "\"Backup\", 5629499534213120".encode('base64').strip()
        key2 = ndb.Key('Table', 6394035673497600, 'Backup', 5629499534213120)

        request_body = "[{{\"backupBqKey\": \"{}\"," \
                       "\"test_param_key\":\"test_value\"}}," \
                       "{{\"backupUrlSafeKey\": \"{}\"}}]". \
            format(bq_key, key2.urlsafe())

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )
        # then
        self.assertEquals(400, response.status_int)
        self.assertIn("There are duplicated backup items", response)

    def test_should_fail_if_backup_key_missing_in_any_item(self):
        # given
        request_body = "[{\"backupUrlSafeKey\": \"value_1\"," \
                       "\"test_param_key\":\"test_value\"}," \
                       "{\"backupUrlSafeKeys\": \"value_2\"}]"

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)

    def test_should_fail_on_non_parseable_backup_key(self):
        # given
        request_body = "[{\"backupUrlSafeKey\": \"wrong_backup_key\"}]"

        expected_error = "{\"status\": \"failed\", \"message\": \"Unable to " \
                         "parse url safe key: wrong_backup_key, error type: " \
                         "ProtocolBufferDecodeError, error " \
                         "message: truncated\", \"httpStatus\": 400}"

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @patch.object(BackupListRestoreService, 'restore', return_value="123")
    def test_should_work_with_backup_bq_key(self, mocked_restore_service):
        # given
        expected_key = ndb.Key("Backup", 5629499534213120,
                               parent=ndb.Key("Table", 6394035673497600))
        backup_bq_key = "\"Table\", 6394035673497600, " \
                        "\"Backup\", 5629499534213120".encode('base64').strip()

        mocked_restore_service.return_value = {"restorationJobId": "restore_id"}

        expected_backup_item = \
            BackupItem(expected_key, {"test_param_key": "test_value"})

        request_body = "[{{\"backupBqKey\": \"{}\"}}]".format(backup_bq_key)

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json'
        )

        # then
        response_json = json.loads(response.body)
        self.assertTrue('restorationJobId' in response_json)

        mocked_restore_service_request = \
            self.__get_mocked_service_argument(mocked_restore_service)
        passed_backup_items = list(mocked_restore_service_request.backup_items)

        self.assertEqual(len(passed_backup_items), 1)
        self.assertEqual(passed_backup_items[0], expected_backup_item)

    def test_should_fail_when_both_keys_are_specified(self):
        # given
        request_body = "[{\"backupUrlSafeKey\": \"key1\"," \
                       " \"backupBqKey\": \"key2\"}]"

        expected_error = "{\"status\": \"failed\", \"message\": \"Please " \
                         "specify either \'backupUrlSafeKey\' or " \
                         "\'backupBqKey\' element in single item.\", " \
                         "\"httpStatus\": 400}"

        # when
        response = self.under_test.post(
            url='/restore/list',
            params=request_body,
            content_type='application/json',
            expect_errors=True
        )

        # then
        self.assertEquals(400, response.status_int)
        self.assertEquals(response.body, expected_error)

    @staticmethod
    def __get_mocked_service_argument(mocked_service):
        return mocked_service.call_args[0][0]
