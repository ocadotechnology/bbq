import datetime

from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb

from src.commons.config.configuration import configuration


class ShouldPerformDatastoreCleanupPredicate(object):

    @staticmethod
    def test(url_safe_key, existing_backups):
        if existing_backups:
            return False
        if ShouldPerformDatastoreCleanupPredicate.\
                __is_table_deleted_before_threshold(url_safe_key):
            return True
        return False

    @staticmethod
    def __is_table_deleted_before_threshold(url_safe_key):
        table = ndb.Key(urlsafe=url_safe_key).get()
        deletion_threshold_in_months = \
            configuration.grace_period_after_source_table_deletion_in_months + 1

        age_threshold_date = \
            datetime.datetime.now() - relativedelta(months=deletion_threshold_in_months)

        if table.last_checked < age_threshold_date:
            return True

        return False
