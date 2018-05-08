import datetime
import logging

from dateutil.relativedelta import relativedelta
from src.backup.datastore.Backup import Backup


class YoungerThan7MonthsFilter(object):
    NUMBER_OF_MONTHS_TO_KEEP = 7

    def filter(self, backups, table_reference):
        sorted_backups = Backup.sort_backups_by_create_time_desc(backups)
        age_threshold_date = datetime.date.today() - \
                             relativedelta(months=self.NUMBER_OF_MONTHS_TO_KEEP)
        young_backups = [b for b in sorted_backups
                         if b.created.date() >= age_threshold_date]

        if young_backups:
            return young_backups

        old_backups_available = len(young_backups) != len(sorted_backups)
        if old_backups_available:
            logging.info(
                'There are no backups for table \'%s\' younger than seven months. '
                'The most recent backup will be retained', table_reference)
            return sorted_backups[:1]

        logging.info('This table seems to have no backups.')
        return sorted_backups
