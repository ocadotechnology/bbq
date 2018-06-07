import webapp2

from commons.json_handler import JsonHandler
from src.bbq_authenticated_handler import BbqAuthenticatedHandler
from src.configuration import configuration
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService


class RestorationJobStatusHandler(JsonHandler):

    def get(self, restoration_job_id):
        # @refactor do not use "default_value='False'" as it's confusing,
        # but still allow user to pass warningsOnly without specifying value
        # because it's boolean flag
        warnings_only = self.request.get('warningsOnly', default_value='False')
        warnings_only = True if warnings_only == '' else False
        restoration_job = RestorationJobStatusService().get_restoration_job(
            restoration_job_id, warnings_only)
        self._finish_with_success(restoration_job)


class RestorationJobStatusAuthenticatedHandler(RestorationJobStatusHandler,
                                               BbqAuthenticatedHandler):

    def __init__(self, request=None, response=None):
        super(RestorationJobStatusAuthenticatedHandler, self). \
            __init__(request, response)


app = webapp2.WSGIApplication([
    webapp2.Route(
        '/restore/jobs/<restoration_job_id:.*>',
        RestorationJobStatusHandler
    ), webapp2.Route(
        '/schedule/restore/jobs/<restoration_job_id:.*>',
        RestorationJobStatusAuthenticatedHandler
    )
], debug=configuration.debug_mode)
