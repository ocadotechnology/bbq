import logging

from google.appengine.api.taskqueue import TransientError, Queue, \
    TaskAlreadyExistsError, DuplicateTaskNameError, TombstonedTaskError, \
    UnknownQueueError, Task
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError

from src.commons.decorators.retry import retry
from error_reporting import ErrorReporting
from src.commons import request_correlation_id
from src.commons.collections import paginated


class Tasks(object):
    # pylint: disable=C0330
    @classmethod
    def log_task_metadata_for(cls, request):
        taskqueue_retry_count_mark = 5
        if 'X-AppEngine-TaskRetryCount' in request.headers:
            task_retry_count = int(
                request.headers['X-AppEngine-TaskRetryCount']
            )
            logging.info('Current TaskRetryCount = %s', task_retry_count)
            if task_retry_count == taskqueue_retry_count_mark:
                message = "Following task fails repeatedly: '{}' " \
                          "(retry count:{}). Please investigate.".format(
                    request.headers['X-AppEngine-TaskName'],
                    task_retry_count)
                logging.warning(message)
                ErrorReporting().report(message)

        if 'X-AppEngine-TaskExecutionCount' in request.headers:
            logging.info(
                'Current TaskExecutionCount = ' +
                request.headers['X-AppEngine-TaskExecutionCount']
            )

    @classmethod
    def create(cls, **kwargs):
        corr_id = request_correlation_id.get()
        if corr_id:
            if 'headers' in kwargs:
                kwargs['headers'][
                    request_correlation_id.HEADER_NAME] = corr_id
            else:
                kwargs['headers'] = {
                    request_correlation_id.HEADER_NAME: corr_id
                }
        return Task(**kwargs)

    @classmethod
    def schedule(cls, queue_name, tasks):
        if not isinstance(tasks, list):
            tasks = [tasks]
        queue = Queue(queue_name)
        page_size = 100
        task_count = 0
        for task_batch in paginated(page_size, tasks):
            cls.__add_single_batch(queue, task_batch)
            task_count += len(task_batch)
        if task_count > 0:
            logging.info("Scheduled %d tasks in max %d batches",
                         task_count, page_size)

    @classmethod
    @retry((DeadlineExceededError, TransientError), tries=6, delay=2, backoff=2)
    def __add_single_batch(cls, queue, task_batch):
        if task_batch:
            try:
                queue.add(task_batch)
                logging.info("Scheduled %d tasks", len(task_batch))
            except (DuplicateTaskNameError,
                    TaskAlreadyExistsError,
                    TombstonedTaskError) as ex:
                logging.warning("Task already added %s. Exception: %s",
                                task_batch, type(ex))
            except UnknownQueueError:
                raise UnknownQueueError(
                    "There is no queue '{}'. Please add it to your queue.yaml "
                    "definition.".format(
                        queue.name))

