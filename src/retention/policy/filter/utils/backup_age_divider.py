import datetime

from dateutil.relativedelta import relativedelta

from src.commons.config.configuration import configuration


class BackupAgeDivider(object):

    @staticmethod
    def divide_backups_by_age(backups):
        age_threshold_date = datetime.date.today() - relativedelta(
            months=configuration.young_old_generation_threshold_in_months)

        young_backups = [b for b in backups
                         if b.created.date() >= age_threshold_date]
        old_backups = [b for b in backups
                       if b.created.date() < age_threshold_date]

        return young_backups, old_backups
