import os

from google.appengine.api.app_identity import app_identity


class Environment(object):
    @classmethod
    def version_id(cls):
        return os.environ['CURRENT_VERSION_ID']

    @classmethod
    def is_debug_mode_allowed(cls):
        """
        This method return information whether application may be run in debug
        mode according to location where application is run (e.g. debug is not
        allowed on production)

        :return:    [True/False] True when application is allowed to run debug
                    mode
        """

        return cls.is_dev()

    @classmethod
    def is_dev_sandbox(cls):
        """
        Class checks whether we're running on local development instance
        :return: [True/False] True if running under dev_appserver.py sandbox
        """

        server_software = os.environ['SERVER_SOFTWARE']
        is_dev_sandbox = server_software.startswith("Development/")

        return is_dev_sandbox

    @classmethod
    def is_dev_version(cls):
        """
        Function checks whether application is run on GAE as development
        version. Remember that this is just an agreement, that any
        AppEngine App version which name starts with "dev-" is meant to be a
        development version of this application.

        :return: [True/False]
        """

        version_id = os.environ['CURRENT_VERSION_ID']
        if version_id.startswith('dev-'):
            return True

        return False

    @classmethod
    def is_dev_project(cls):
        return 'dev-' in app_identity.get_application_id()

    @classmethod
    def is_no_server(cls):
        server_software = os.environ.get('SERVER_SOFTWARE', None)
        if server_software is None:
            return True
        else:
            return False

    @classmethod
    def is_local(cls):
        return Environment.is_no_server() or Environment.is_dev_sandbox()

    @classmethod
    def is_dev(cls):
        return Environment.is_no_server() or Environment.is_dev_sandbox() \
               or Environment.is_dev_version() or Environment.is_dev_project()

    @classmethod
    def get_name(cls):
        if cls.is_local():
            return "local"
        elif cls.is_dev():
            return "dev"
        else:
            return "prod"

    @classmethod
    def get_domain(cls, project_id):
        if cls.is_local():
            return "http://localhost:8080"
        else:
            return "https://{}.appspot.com".format(project_id)
