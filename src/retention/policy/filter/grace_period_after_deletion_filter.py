import datetime
import logging

from dateutil.relativedelta import relativedelta

from src.big_query.big_query_table_metadata import BigQueryTableMetadata


class GracePeriodAfterDeletionFilter(object):
    GRACE_PERIOD_AFTER_DELETION_IN_MONTHS = 7

    def filter(self, backups, table_reference):
        if self.__should_keep_backups(backups, table_reference):
            return backups
        else:
            return []

    def __should_keep_backups(self, backups, table_reference):
        age_threshold_date = datetime.date.today() - relativedelta(
            months=self.GRACE_PERIOD_AFTER_DELETION_IN_MONTHS)
        old_backups = [b for b in backups
                       if b.created.date() < age_threshold_date]

        if not old_backups:
            return True

        source_table_last_seen = backups[0].get_table().last_checked.date()
        deleted_within_grace_period = \
            source_table_last_seen >= age_threshold_date
        if deleted_within_grace_period:
            logging.info(
                'Looks like table %s is deleted. It was last seen '
                'on %s which is within grace period.'
                'Will keep backups until the period is over',
                table_reference, source_table_last_seen)
            return True

        if self.__source_table_exists(table_reference):
            logging.info("Table %s was last seen on %s "
                         "but it looks like it still exists in BigQuery.",
                         table_reference, source_table_last_seen)
            return True

        logging.info(
            'Table %s was last seen on %s. It looks like its '
            'gone for long. No backups will be retained',
            table_reference, source_table_last_seen)
        return False

    def __source_table_exists(self, table_reference):
        return BigQueryTableMetadata.get_table_by_reference(table_reference).table_exists()
