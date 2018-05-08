import logging

import jsonpickle
from google.appengine.api.taskqueue import UnknownQueueError, Task

from src.tasks import Tasks
from src.backup.copy_job_async.copy_job.copy_job_task_name import \
    CopyJobTaskName
from src.configuration import configuration


class TaskCreator(object):

    @classmethod
    def create_copy_job(cls, copy_job_request):
        task_name = CopyJobTaskName(copy_job_request).create()
        queue_name = copy_job_request.copy_job_type_id + '-copy-job'
        logging.info("Schedule copy_job_task for '%s' in queue '%s'",
                     copy_job_request, queue_name)
        # try:
        task = Task(
            method='POST',
            url='/tasks/copy_job_async/copy_job',
            name=task_name,
            params={"copyJobRequest": jsonpickle.encode(copy_job_request)},
        )
        Tasks.schedule(queue_name, task)

        logging.info('Task %s enqueued, ETA %s', task.name, task.eta)

    @classmethod
    def create_copy_job_result_check(cls, result_check_request):
        assert result_check_request.retry_count >= 0
        queue_name = result_check_request.copy_job_type_id + '-result-check'
        logging.info(
            "Schedule copy_job_result_check task for %s (project: '%s') "
            "jobId: '%s' (retry count:'%s') in queue '%s'",
            result_check_request, result_check_request.project_id,
            result_check_request.job_id, result_check_request.retry_count,
            queue_name)

        task = Task(
            method='POST',
            url='/tasks/copy_job_async/result_check',
            countdown=configuration.copy_job_result_check_countdown_in_sec,
            params={
                "resultCheckRequest": jsonpickle.encode(result_check_request)}
        )
        Tasks.schedule(queue_name, task)

        logging.info('Task %s enqueued, ETA %s', task.name, task.eta)

    @classmethod
    def create_post_copy_action(cls, copy_job_type_id, post_copy_action_request,
                                job_json):
        queue_name = copy_job_type_id + '-post-copy-action'
        logging.info(
            "Creating task on queue '%s' for post copy action ('%s'): %s ",
            queue_name, copy_job_type_id, post_copy_action_request)

        task = Task(
            method='POST',
            url=post_copy_action_request.url,
            payload=jsonpickle.encode({
                "data": post_copy_action_request.data,
                "jobJson": job_json
            })
        )
        Tasks.schedule(queue_name, task)

        logging.info('Task %s enqueued, ETA %s', task.name, task.eta)
