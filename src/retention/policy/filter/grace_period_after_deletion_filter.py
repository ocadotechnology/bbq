import datetime
import logging

from dateutil.relativedelta import relativedelta
from apiclient.errors import HttpError

from src.commons.big_query.big_query_table_metadata import BigQueryTableMetadata
from src.commons.config.configuration import configuration


class GracePeriodAfterDeletionFilter(object):

    def filter(self, backups, table_reference):
        if self.__should_keep_backups(backups, table_reference):
            return backups
        else:
            return []

    def __should_keep_backups(self, backups, table_reference):
        age_threshold_date = datetime.date.today() - relativedelta(
            months=configuration.grace_period_after_source_table_deletion_in_months)
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
        try:
            return BigQueryTableMetadata.get_table_by_reference(
                table_reference).table_exists()
        except HttpError as error:
            if self.__is_getting_partition_from_non_partitioned_error(table_reference, error):
                return False
            raise error

    @staticmethod
    def __is_getting_partition_from_non_partitioned_error(table_reference, error):
        return table_reference.is_partition() \
               and "Cannot read partition information from a table that is not partitioned" in error.content
