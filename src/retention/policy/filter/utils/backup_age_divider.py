import datetime

from dateutil.relativedelta import relativedelta

NUMBER_OF_MONTHS_TO_KEEP = 7


class BackupAgeDivider(object):

    @staticmethod
    def divide_backups_by_age(backups):
        age_threshold_date = datetime.date.today() - relativedelta(
            months=NUMBER_OF_MONTHS_TO_KEEP)

        young_backups = [b for b in backups
                         if b.created.date() >= age_threshold_date]
        old_backups = [b for b in backups
                       if b.created.date() < age_threshold_date]

        return young_backups, old_backups
