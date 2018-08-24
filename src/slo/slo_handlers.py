import logging

import webapp2

from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class XDaysSLIMainHandler(webapp2.RequestHandler):

    def get(self):
        logging.info("Recalculating X days SLIs has been started.")
        Tasks.schedule("default", self.create_slo_recalculation_tasks())

    @staticmethod
    def create_slo_recalculation_tasks():
        return [
            Tasks.create(
                method='POST',
                url='/slo/recalculate_x_days',
                params={'x_days': x_days})
            for x_days in [3, 4, 5, 7]
        ]


app = webapp2.WSGIApplication([
    ('/cron/slo/calculate', XDaysSLIMainHandler)
], debug=configuration.debug_mode)
