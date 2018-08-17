import collections
import logging

from src.commons.error_reporting import ErrorReporting


class ShouldPerformRetentionPredicate(object):
    @classmethod
    def test(cls, backups):
        if len(backups) == 0:
            logging.info("Backup list is empty")
            return False

        counter = collections.Counter([x.table_id for x in backups])
        for table_id, count in counter.most_common(1):
            if count > 1:
                ErrorReporting().report("There are multiple entities in "
                                        "datastore referencing to the same "
                                        "backup table in bigquery: '{}'"
                                        .format(table_id))
                return False
        return True
