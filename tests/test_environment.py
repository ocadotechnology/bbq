import unittest

import os

from google.appengine.api.app_identity import app_identity
from mock import mock, patch

from src.environment import Environment


class TestEnvironment(unittest.TestCase):

    @mock.patch.dict(os.environ, {'CURRENT_VERSION_ID': 'version_id'})
    def test_version_id_happy_path(self):
        # given & when
        result = Environment.version_id()

        # then
        self.assertEqual(result, 'version_id')

    @patch.object(Environment, 'is_dev', return_value=True)
    def test_is_debug_mode_allowed_if_dev(self, _):
        # given & when
        result = Environment.is_debug_mode_allowed()

        # then
        self.assertEqual(result, True)

    @patch.object(Environment, 'is_dev', return_value=False)
    def test_is_debug_mode_not_allowed_if_not_dev(self, _):
        # given & when
        result = Environment.is_debug_mode_allowed()

        # then
        self.assertEqual(result, False)

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

    @mock.patch.dict(os.environ, {'CURRENT_VERSION_ID': 'dev-version'})
    def test_is_dev_version_if_version_starts_with_dev(self):
        # given & when
        result = Environment.is_dev_version()

        # then
        self.assertEqual(result, True)

    @mock.patch.dict(os.environ, {'CURRENT_VERSION_ID': 'prod-version'})
    def test_is_not_dev_version_if_version_do_not_start_with_dev(self):
        # given & when
        result = Environment.is_dev_version()

        # then
        self.assertEqual(result, False)

    @patch.object(app_identity, 'get_application_id')
    def test_is_dev_project_if_app_id_starts_with_dev(self, get_application_id):
        # given
        get_application_id.return_value = 'dev-version'

        # when
        result = Environment.is_dev_project()

        # then
        self.assertEqual(result, True)

    @patch.object(app_identity, 'get_application_id')
    def test_is_not_dev_project_if_app_id_do_not_start_with_dev(
            self, get_application_id):
        # given
        get_application_id.return_value = 'prod-version'

        # when
        result = Environment.is_dev_project()

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

    @patch.object(Environment, 'is_no_server', return_value=False)
    @patch.object(Environment, 'is_dev_sandbox', return_value=False)
    @patch.object(Environment, 'is_dev_version', return_value=False)
    @patch.object(Environment, 'is_dev_project', return_value=False)
    def test_is_not_dev_if_not_local_and_not_dev_version_and_project(
            self, _, _1, _2, _3):
        # given & when
        result = Environment.is_local()

        # then
        self.assertEqual(result, False)

    @patch.object(Environment, 'is_local', return_value=True)
    def test_that_get_name_return_local_is_local(self, _):
        # given & when
        result = Environment.get_name()

        # then
        self.assertEqual(result, 'local')

    @patch.object(Environment, 'is_local', return_value=False)
    @patch.object(Environment, 'is_dev', return_value=True)
    def test_that_get_name_return_dev_if_is_dev_and_not_local(self, _, _1):
        # given & when
        result = Environment.get_name()

        # then
        self.assertEqual(result, 'dev')

    @patch.object(Environment, 'is_local', return_value=False)
    @patch.object(Environment, 'is_dev', return_value=False)
    def test_that_get_name_return_prod_if_not_local_and_dev(self, _, _1):
        # given & when
        result = Environment.get_name()

        # then
        self.assertEqual(result, 'prod')

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
