import unittest

from src.commons.table_reference import TableReference


class TestTableReference(unittest.TestCase):

    def test_table_str(self):
        # given
        table = TableReference("project1", "dataset1", "table1")
        # when
        table_string = str(table)
        # then
        self.assertEqual(table_string, "project1:dataset1.table1")

    def test_partition_str(self):           # given
        table_partition = TableReference("project1", "dataset1", "table1",
                                         "partition1")
        # when
        table_partition_string = str(table_partition)
        # then
        self.assertEqual(table_partition_string,
                         "project1:dataset1.table1$partition1")

    def test_parse_tab_ref(self):
        #when
        actual_table_ref = TableReference.parse_tab_ref("proj321:dataset123.tableabc")

        #then
        self.assertEqual(TableReference("proj321", "dataset123", "tableabc"), actual_table_ref)

    def test_parse_tab_ref_for_partitioned_table(self):
        #when
        actual_table_ref = TableReference.parse_tab_ref("proj321:dataset123.tableabc$20180226")

        #then
        self.assertEqual(TableReference("proj321", "dataset123", "tableabc", "20180226"), actual_table_ref)

    def test_parse_tab_ref_do_not_raise_exception_if_it_matches_naming_convention(self):
        TableReference.parse_tab_ref("proj321:dataset123.tableabc$20180226")
        TableReference.parse_tab_ref("proj321:dataset123.tableabc")
        TableReference.parse_tab_ref("proj321:dataset123.tableabc_123")
        TableReference.parse_tab_ref("proj321:datas_et123.tableabc123")
        TableReference.parse_tab_ref("proj-321:dataset123.tableabc123")
        TableReference.parse_tab_ref("dev-project-bbq:smoke_test_EU.epd_none_direct___cpd_indirect___rm_manual___sharded_20170804")

    def test_parse_tab_ref_raise_exception_if_doesnt_match_naming_convention(self):
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321dataset123.tableabc")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321dataset123tableabc")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321:dataset123.tableabc$dsfsf")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321dataset123tableabc$20180501")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321:dataset123.table-abc$20180501")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj321:datase-t123.tableabc$20180501")
        with self.assertRaises(Exception):
            TableReference.parse_tab_ref("proj_321:dataset123.tableabc$20180501")
