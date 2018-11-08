import unittest

from src.commons.big_query.copy_job_async.copy_job_result import CopyJobResult
from src.commons.table_reference import TableReference
from tests.commons.copy_job_async.result_check.job_result_example import \
    JobResultExample


class TestCopyJobResult(unittest.TestCase):

    def test_source_table_reference(self):
        # given
        copy_job_result = CopyJobResult(JobResultExample.DONE)
        # when
        source_table_reference = copy_job_result.source_table_reference
        # then
        self.assertEqual(source_table_reference,
                         TableReference(project_id="source_project_id",
                                        dataset_id="source_dataset_id",
                                        table_id="source_table_id",
                                        partition_id="123"))

    def test_target_table_reference(self):
        # given
        copy_job_result = CopyJobResult(JobResultExample.DONE)
        # when
        target_table_reference = copy_job_result.target_table_reference
        # then
        self.assertEqual(target_table_reference,
                         TableReference(project_id="target_project_id",
                                        dataset_id="target_dataset_id",
                                        table_id="target_table_id"))

    def test_create_disposition(self):
        # given
        copy_job_result = CopyJobResult(JobResultExample.DONE)
        # when
        create_disposition = copy_job_result.create_disposition
        # then
        self.assertEqual(create_disposition, 'CREATE_IF_NEEDED')

    def test_write_disposition(self):
        # given
        copy_job_result = CopyJobResult(JobResultExample.DONE)
        # when
        write_disposition = copy_job_result.write_disposition
        # then
        self.assertEqual(write_disposition, 'WRITE_TRUNCATE')
