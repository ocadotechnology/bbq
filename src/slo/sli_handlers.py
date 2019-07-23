import logging

import webapp2

from src.commons.config.configuration import configuration
from src.commons.tasks import Tasks


class SLIMainHandler(webapp2.RequestHandler):

    def get(self):
        logging.info("Recalculating SLIs has been started.")
        Tasks.schedule("sli-worker", self.create_slo_recalculation_tasks())

    @staticmethod
    def create_slo_recalculation_tasks():
        tasks = [
            Tasks.create(method='POST',
                         url='/sli/latency_for_x_days',
                         params={'x_days': x_days})
            for x_days in [4, 5, 7]
        ]
        tasks.append(
            Tasks.create(
                method='POST',
                url='/sli/quality'))
        return tasks


app = webapp2.WSGIApplication([
    ('/cron/sli/calculate', SLIMainHandler)
], debug=configuration.debug_mode)
