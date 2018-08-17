import unittest

from google.appengine.ext import ndb

from src.commons.exceptions import ParameterValidationException
from src.restore.list.backup_key_parser import BackupKeyParser


class TestBackupKeyParser(unittest.TestCase):

    def test_should_parse_url_safe_key(self):
        # given
        key = ndb.Key("Backup", 123123)

        # when
        actual_key = BackupKeyParser.parse_url_safe_key(key.urlsafe())

        # then
        self.assertEqual(key, actual_key)

    def test_should_throw_error_on_incorrect_padding_key(self):
        # given
        expected_error = "Unable to parse url safe key: wrong_url_key, error " \
                         "type: TypeError, error message: Incorrect padding"

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupKeyParser.parse_url_safe_key("wrong_url_key")

        # then
        self.assertEquals(expected_error, context.exception.message)

    def test_should_throw_error_on_wrong_parse_key(self):
        # given
        expected_error = "Unable to parse url safe key: wrong_backup_key, " \
                         "error type: ProtocolBufferDecodeError, error " \
                         "message: truncated"

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupKeyParser.parse_url_safe_key("wrong_backup_key")

        # then
        self.assertEquals(expected_error, context.exception.message)

    def test_should_parse_backup_bq_key(self):
        # given
        expected_key = ndb.Key("Backup", 5629499534213120,
                               parent=ndb.Key("Table", 6394035673497600))
        backup_bq_key = "\"Table\", 6394035673497600, " \
                        "\"Backup\", 5629499534213120".encode('base64')

        # when
        actual_key = BackupKeyParser.parse_bq_key(backup_bq_key)

        # then
        self.assertEqual(expected_key, actual_key)

    def test_should_throw_error_on_non_base64_backup_bq_key(self):
        # given
        expected_error = "Unable to parse backup BQ key: nonbase64, error " \
                         "type: Error, error message: Incorrect padding"
        backup_bq_key = "nonbase64"

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupKeyParser.parse_bq_key(backup_bq_key)

        # then
        self.assertEquals(expected_error, context.exception.message)

    def test_should_throw_error_on_wrong_number_of_parts(self):
        # given
        expected_error = "Unable to parse backup BQ key: cGFydDEscGFydDIsIHB" \
                         "hcnQz\n, key doesn't consist of 4 parts"
        backup_bq_key = "part1,part2, part3".encode('base64')

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupKeyParser.parse_bq_key(backup_bq_key)

        # then
        self.assertEquals(expected_error, context.exception.message)

    def test_should_throw_error_on_key_with_non_int_id(self):
        # given
        expected_error = "Unable to parse backup BQ key: cGFydDEsMTIzLHBhcn" \
                         "QzLG5vbi1pbnQ=\n, error type: ValueError, error " \
                         "message: invalid literal for int() with base 10:" \
                         " 'non-int'"
        backup_bq_key = "part1,123,part3,non-int".encode('base64')

        # when
        with self.assertRaises(ParameterValidationException) as context:
            BackupKeyParser.parse_bq_key(backup_bq_key)

        # then
        self.assertEquals(expected_error, context.exception.message)

