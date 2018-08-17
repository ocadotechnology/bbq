import os
import unittest

from mock import mock, patch

from src.commons.config.environment import Environment


class TestEnvironment(unittest.TestCase):

    @mock.patch.dict(os.environ, {'CURRENT_VERSION_ID': 'version_id'})
    def test_version_id_happy_path(self):
        # given & when
        result = Environment.version_id()

        # then
        self.assertEqual(result, 'version_id')

    @mock.patch.dict(os.environ, {'SERVER_SOFTWARE': 'Development/what_ever'})
    def test_is_dev_sandbox_if_server_name_starts_with_development(self):
        # given & when
        result = Environment.is_dev_sandbox()

        # then
        self.assertEqual(result, True)

    @mock.patch.dict(os.environ, {'SERVER_SOFTWARE': 'Prod/what_ever'})
    def test_is_not_dev_sandbox_if_server_name_do_not_start_with_development(
            self):
        # given & when
        result = Environment.is_dev_sandbox()

        # then
        self.assertEqual(result, False)

    def test_that_is_no_server_if_server_name_has_no_name(self):
        # given
        os.environ.clear()

        # given
        result = Environment.is_no_server()

        # then
        self.assertEqual(result, True)

    @mock.patch.dict(os.environ, {'SERVER_SOFTWARE': 'Prod/what_ever'})
    def test_is_server_if_any_found(self):
        # given & when
        result = Environment.is_no_server()

        # then
        self.assertEqual(result, False)

    @patch.object(Environment, 'is_no_server', return_value=False)
    @patch.object(Environment, 'is_dev_sandbox', return_value=False)
    def test_is_not_local_if_server_exists_and_no_sandbox(self, _, _1):
        # given & when
        result = Environment.is_local()

        # then
        self.assertEqual(result, False)

    @patch.object(Environment, 'is_local', return_value=True)
    def test_get_domain_return_localhost_if_local(self, _):
        # given
        project_id = 'project_id'

        # when
        result = Environment.get_domain(project_id)

        # then
        self.assertEqual(result, "http://localhost:8080")

    @patch.object(Environment, 'is_local', return_value=False)
    def test_get_domain_return_appspot_url_if_not_local(self, _):
        # given
        project_id = 'project_id'

        # when
        result = Environment.get_domain(project_id)

        # then
        expected_result = "https://{}.appspot.com".format(project_id)
        self.assertEqual(result, expected_result)

