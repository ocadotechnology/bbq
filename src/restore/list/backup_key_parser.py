from google.appengine.ext import ndb
from google.net.proto.ProtocolBuffer import ProtocolBufferDecodeError

from src.commons.exceptions import ParameterValidationException
from binascii import Error


class BackupKeyParser(object):

    @staticmethod
    def parse_url_safe_key(url_safe_key):
        try:
            return ndb.Key(urlsafe=url_safe_key)
        except (TypeError, ProtocolBufferDecodeError), e:
            raise ParameterValidationException(
                "Unable to parse url safe key: {}, error type: {}, "
                "error message: {}".format(url_safe_key,
                                           type(e).__name__, e.message))

    @staticmethod
    def parse_bq_key(backup_bq_key):
        try:
            key_parts = backup_bq_key.decode('base64') \
                .replace("\"", "").replace(" ", "").split(",")
            if len(key_parts) != 4:
                raise ParameterValidationException(
                    "Unable to parse backup BQ key: {}, "
                    "key doesn't consist of 4 parts".format(backup_bq_key))
            table_kind = key_parts[0]
            table_id = int(key_parts[1])
            backup_kind = key_parts[2]
            backup_id = int(key_parts[3])
            return ndb.Key(backup_kind, backup_id,
                           parent=ndb.Key(table_kind, table_id))
        except (Error, ValueError), e:
            raise ParameterValidationException(
                "Unable to parse backup BQ key: {}, error type: {}, "
                "error message: {}".format(backup_bq_key,
                                           type(e).__name__, e.message))
