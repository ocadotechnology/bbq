import unittest

from freezegun import freeze_time

from src.commons.big_query.copy_job_async.copy_job.copy_job_request \
    import CopyJobRequest
from src.commons.big_query.copy_job_async.copy_job.copy_job_task_name \
    import CopyJobTaskName
from src.commons.big_query.big_query_table import BigQueryTable


class TestCopyJobNameCreator(unittest.TestCase):

    @freeze_time("2017-12-06")
    def test_creating_task_name(self):
        # given
        copy_job_request = CopyJobRequest(
            task_name_suffix='task_name_suffix',
            copy_job_type_id="unknown-copying",
            source_big_query_table=BigQueryTable('source_project',
                                                 'source_dataset',
                                                 'source_table'),
            target_big_query_table=BigQueryTable('target_project',
                                                 'target_dataset',
                                                 'target_table'),
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY",
            retry_count=0
        )

        # when
        copy_job_task_name = CopyJobTaskName(copy_job_request).create()

        # then
        self.assertEqual(copy_job_task_name, '2017-12-06_source_project_source_dataset_source_table_0_task_name_suffix')

    @freeze_time("2017-12-06")
    def test_return_none_if_calculated_name_is_too_long(self):
        # given
        task_name_suffix = "x" * 501

        copy_job_request = CopyJobRequest(
            task_name_suffix=task_name_suffix,
            copy_job_type_id="unknown-copying",
            source_big_query_table=BigQueryTable('source_project',
                                                 'source_dataset',
                                                 'source_table'),
            target_big_query_table=BigQueryTable('target_project',
                                                 'target_dataset',
                                                 'target_table'),
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_EMPTY",
            retry_count=0
        )

        # when
        copy_job_task_name = CopyJobTaskName(copy_job_request).create()

        # then
        self.assertIsNone(copy_job_task_name)
