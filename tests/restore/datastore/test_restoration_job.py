import unittest
from datetime import datetime

from freezegun import freeze_time
from google.appengine.ext import ndb, testbed

from src.restore.datastore.restoration_job import RestorationJob, \
    DuplicatedRestorationJobIdException

TEST_RESTORATION_JOB_ID = "111"
CREATE_DISPOSITION = "CREATE_IF_NEEDED"
WRITE_DISPOSITION = "WRITE_EMPTY"


class TestRestorationJob(unittest.TestCase):

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def test_restoration_job_saved_in_datastore(self):
        # when
        RestorationJob.create(restoration_job_id=TEST_RESTORATION_JOB_ID,
                              create_disposition=CREATE_DISPOSITION,
                              write_disposition=WRITE_DISPOSITION)
        # then
        self.assertIsNotNone(RestorationJob.get_by_id(TEST_RESTORATION_JOB_ID))

    def test_restoration_job_has_initial_count_0(self):
        # when
        RestorationJob.create(restoration_job_id=TEST_RESTORATION_JOB_ID,
                              create_disposition=CREATE_DISPOSITION,
                              write_disposition=WRITE_DISPOSITION)
        # then
        self.assertEqual(
            RestorationJob.get_by_id(TEST_RESTORATION_JOB_ID).items_count, 0)

    def test_restoration_job_has_passed_write_disposition(self):
        # when
        RestorationJob.create(restoration_job_id=TEST_RESTORATION_JOB_ID,
                              create_disposition=CREATE_DISPOSITION,
                              write_disposition=WRITE_DISPOSITION)
        # then
        self.assertEqual(
            RestorationJob.get_by_id(TEST_RESTORATION_JOB_ID).write_disposition,
            WRITE_DISPOSITION)

    def test_restoration_job_has_passed_create_disposition(self):
        # when
        RestorationJob.create(restoration_job_id=TEST_RESTORATION_JOB_ID,
                              create_disposition=CREATE_DISPOSITION,
                              write_disposition=WRITE_DISPOSITION)
        # then
        self.assertEqual(
            RestorationJob.get_by_id(TEST_RESTORATION_JOB_ID).create_disposition,
            CREATE_DISPOSITION)

    @freeze_time("2012-01-14 03:21:34")
    def test_restoration_job_has_now_as_initial_created_time(self):
        # when
        RestorationJob.create(restoration_job_id=TEST_RESTORATION_JOB_ID,
                              create_disposition=CREATE_DISPOSITION,
                              write_disposition=WRITE_DISPOSITION)
        # then
        self.assertEqual(
            RestorationJob.get_by_id(TEST_RESTORATION_JOB_ID).created,
            datetime(2012, 1, 14, 3, 21, 34))

    def test_restoration_job_return_entity_key(self):
        # when
        entity_key = RestorationJob.create(
            restoration_job_id=TEST_RESTORATION_JOB_ID,
            create_disposition=CREATE_DISPOSITION,
            write_disposition=WRITE_DISPOSITION)
        # then
        self.assertIsNotNone(entity_key.get())

    def test_increment(self):
        # given
        restoration_job = self.__create_restoration_job()
        # when
        restoration_job.increment_count_by(count=2)
        # then
        ndb.get_context().clear_cache()
        self.assertEqual(restoration_job.key.get().items_count, 2)

    def test_double_increment(self):
        # given
        restoration_job = self.__create_restoration_job()
        # when
        restoration_job.increment_count_by(count=2)
        restoration_job.increment_count_by(count=3)
        # then
        ndb.get_context().clear_cache()
        self.assertEqual(restoration_job.key.get().items_count, 5)

    def test_create_if_not_exist_should_throw_exception_if_already_exist(self):
        # given
        RestorationJob.create(
            restoration_job_id=TEST_RESTORATION_JOB_ID,
            create_disposition=CREATE_DISPOSITION,
            write_disposition=WRITE_DISPOSITION)
        # when then
        with self.assertRaises(DuplicatedRestorationJobIdException):
            RestorationJob.create(
                restoration_job_id=TEST_RESTORATION_JOB_ID,
                create_disposition=CREATE_DISPOSITION,
                write_disposition=WRITE_DISPOSITION)

    def __create_restoration_job(self):
        return RestorationJob.create(
            restoration_job_id=TEST_RESTORATION_JOB_ID,
            create_disposition=CREATE_DISPOSITION,
            write_disposition=WRITE_DISPOSITION).get()
