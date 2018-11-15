import unittest
from datetime import datetime

from google.appengine.ext import testbed, ndb
from mock import patch

from src.commons.exceptions import NotFoundException
from src.commons.config.environment import Environment
from src.restore.datastore.restoration_job import RestorationJob
from src.restore.datastore.restore_item import RestoreItem, TableReferenceEntity
from src.restore.status.restoration_job_status_service import \
    RestorationJobStatusService

TEST_RESTORATION_JOB_ID = "111"


class TestRestorationJobStatusService(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def test_retrieving_restoration_job_without_restoration_items(self):
        # given
        job_key = self.create_restoration_job_with_count(1)

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], 'In progress')
        self.assertFalse('result' in result['status'])
        self.assertEqual(result['itemResults']['unknown'], 1)

    def test_retrieving_restoration_job_with_completed_date(self):
        # given
        job_key = self.create_restoration_job_with_count(1)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE,
                                           datetime(2017, 12, 22, 10, 05,
                                                    15)).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], 'Done')
        self.assertEqual(result['status']['result'], 'Success')
        self.assertEqual(result['itemResults']['done'], 1)
        restoration_items = result['restorationItems']
        self.assertEqual(1, len(restoration_items))
        self.assertEqual(restoration_items[0]['completed'],
                         "2017-12-22T10:05:15")

    def test_retrieving_restoration_job_with_empty_completed_date(self):
        # given
        job_key = self.create_restoration_job_with_count(1)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], 'Done')
        self.assertEqual(result['status']['result'], 'Success')
        self.assertEqual(result['itemResults']['done'], 1)
        restoration_items = result['restorationItems']
        self.assertEqual(1, len(restoration_items))
        self.assertIsNone(restoration_items[0]['completed'])

    def test_retrieving_restoration_job_with_custom_parameters(self):
        # given
        job_key = self.create_restoration_job_with_count(1)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS,
                                           custom_parameters=
                                           '{"external-id": "13-424"}').put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], 'In progress')
        self.assertFalse('result' in result['status'])
        self.assertEqual(result['itemResults']['inProgress'], 1)
        restoration_items = result['restorationItems']
        self.assertEqual(1, len(restoration_items))
        self.assertEqual(restoration_items[0]['customParameters'],
                         {"external-id": "13-424"})

    def test_should_accept_incorrect_json_as_custom_parameters(self):
        # given
        job_key = self.create_restoration_job_with_count(1)
        incorrect_json = "[<xml/>]\""
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE,
                                           custom_parameters=incorrect_json) \
            .put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        restoration_items = result['restorationItems']
        self.assertEqual(1, len(restoration_items))
        self.assertEqual(restoration_items[0]['customParameters'],
                         incorrect_json)

    def test_retrieving_restoration_job_with_empty_custom_parameters(self):
        # given
        job_key = self.create_restoration_job_with_count(1)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        restoration_items = result['restorationItems']
        self.assertEqual(1, len(restoration_items))
        self.assertIsNone(restoration_items[0]['customParameters'])

    def test_status_should_contain_information_about_incomplete_items(self):
        # given
        job_key = self.create_restoration_job_with_count(3)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], "In progress")

    def test_different_status_results(self):
        # given
        job_key = self.create_restoration_job_with_count(10)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id())

        # then
        self.assertEqual(result['status']['state'], 'In progress')
        self.assertFalse('result' in result['status'])
        self.assertEqual(result['itemResults']['done'], 1)
        self.assertEqual(result['itemResults']['inProgress'], 2)
        self.assertEqual(result['itemResults']['failed'], 3)
        self.assertEqual(result['itemResults']['unknown'], 4)
        self.assertEqual(6, len(result['restorationItems']))

    def test_warnings_only_mode_should_filter_out_in_progress_and_done(self):
        # given
        job_key = self.create_restoration_job_with_count(10)
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_DONE).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_IN_PROGRESS).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()
        self.__create_restore_item_example(job_key,
                                           RestoreItem.STATUS_FAILED).put()

        # when
        result = RestorationJobStatusService().get_restoration_job(job_key.id(),
                                                                   True)

        # then
        self.assertEqual(result['status']['state'], 'In progress')
        self.assertFalse('result' in result['status'])
        self.assertEqual(result['itemResults']['done'], 1)
        self.assertEqual(result['itemResults']['inProgress'], 2)
        self.assertEqual(result['itemResults']['failed'], 3)
        self.assertEqual(result['itemResults']['unknown'], 4)
        self.assertEqual(3, len(result['restorationItems']))

    def create_restoration_job_with_count(self, item_count):
        job_key = RestorationJob.create(TEST_RESTORATION_JOB_ID,
                                        create_disposition="CREATE_IF_NEEDED",
                                        write_disposition="WRITE_EMPTY")
        job_key.get().increment_count_by(item_count)
        return job_key

    def test_should_raise_not_found_exception_for_not_existing_job_id(self):
        # given
        not_existing_key = ndb.Key(RestorationJob, 'not_existing_id')

        # when
        with self.assertRaises(NotFoundException) as context:
            RestorationJobStatusService() \
                .get_restoration_job(not_existing_key.id())
        self.assertEqual(
            context.exception.message, "Restoration job with id: "
                                       "'not_existing_id' doesn't exists")

    @patch.object(Environment, 'get_domain', return_value='http://domain.com')
    def test_get_status_endpoint_return_url_with_job_id(self, _):
        # given
        restoration_job_id = "123"

        # when
        result = RestorationJobStatusService().get_status_endpoint(
            restoration_job_id)

        # then
        self.assertEqual('http://domain.com/restore/jobs/123',
                         result)

    @patch.object(Environment, 'get_domain', return_value='http://domain.com')
    def test_get_warnings_only_status_endpoint_return_url_with_job_id(self, _):
        # given
        restoration_job_id = "123"

        # when
        result = RestorationJobStatusService().get_warnings_only_status_endpoint(
            restoration_job_id)

        # then
        self.assertEqual('http://domain.com/restore/jobs/123?warningsOnly',
                         result)

    @staticmethod
    def __create_restore_item_example(restoration_job_key,
                                      status,
                                      completed=None,
                                      custom_parameters=None):
        return RestoreItem(restoration_job_key=restoration_job_key,
                           status=status,
                           completed=completed,
                           source_table=TableReferenceEntity(
                               project_id='project-abc',
                               dataset_id='dataset_xyz',
                               table_id='23423_table-43-logs_213213',
                               partition_id='20171113'),
                           target_table=TableReferenceEntity(
                               project_id='target-project',
                               dataset_id='dataset_original',
                               table_id='table-43-logs',
                               partition_id='20171113'),
                           custom_parameters=custom_parameters
                           )
