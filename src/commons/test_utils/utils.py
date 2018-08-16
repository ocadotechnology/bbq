import logging
import os
from google.appengine.ext import testbed


def init_testbed_queue_stub(testbed_instance):
    path = os.path.join(os.getcwd(), 'config')
    logging.debug("queue.yaml path: %s", path)
    testbed_instance.init_taskqueue_stub(root_path=path)
    return testbed_instance.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
