import os


class Environment(object):
    @classmethod
    def version_id(cls):
        return os.environ['CURRENT_VERSION_ID']

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
    def get_domain(cls, project_id):
        if cls.is_local():
            return "http://localhost:8080"
        else:
            return "https://{}.appspot.com".format(project_id)
