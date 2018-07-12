import os
import sys
import subprocess
import click

class IsolationTest(object):

    def __init__(self, test_runner, google_cloud_sdk, test_path, v):
        self.test_runner_absolute_path = os.path.abspath(test_runner)
        self.google_cloud_sdk = google_cloud_sdk
        self.test_path = test_path
        if v:
            self.std_output_and_error_redirect = None
        else:
            self.std_output_and_error_redirect = subprocess.PIPE

        self.errors_count = 0

    @staticmethod
    @click.command()
    @click.option('--test_runner',
                  required=True,
                  type=click.Path(exists=True),
                  help='Full path to test_runner.py'
                 )
    @click.option('--google_cloud_sdk',
                  required=True,
                  type=click.Path(exists=True),
                  help='Full path to google-cloud-sdk'
                 )
    @click.option('--test_path',
                  required=True,
                  type=click.Path(exists=True),
                  help='Path to tests'
                 )
    @click.option('--v',
                  flag_value=True,
                  help='Verbose mode. '
                       'Displays more detailed output of test executions'
                 )
    def run(test_runner, google_cloud_sdk, test_path, v):
        IsolationTest(test_runner, google_cloud_sdk, test_path, v)\
            .execute_all_tests_recursively()

    def execute_all_tests_recursively(self):

        print "CHECKING TESTS DIRECTORY: {}".format(self.test_path)
        for dirpath, _, _ in os.walk(self.test_path):
            for test_file in os.listdir(dirpath):
                if test_file.startswith('test') and test_file.endswith('.py'):
                    self.execute_single_file_tests(self.test_path, test_file)

        if self.errors_count == 0:
            print "DONE. Everyting fine."
            sys.exit(os.EX_OK)
        else:
            print "ERROR. There {} {} {}, that went wrong in isolation".format(
                'is' if self.errors_count == 1 else "are",
                self.errors_count,
                'test file' if self.errors_count == 1 else "test files"
            )
            sys.exit(os.O_WRONLY)

    @staticmethod
    def get_tests_directories():
        for dirpath, _, _ in os.walk('.'):
            if dirpath.endswith("tests") \
                    and not IsolationTest.contains_lib_directory(dirpath):
                yield os.path.abspath(dirpath)

    @staticmethod
    def contains_lib_directory(dirpath):
        for directory in dirpath.split('/'):
            if directory == 'lib':
                return True
        return False

    def execute_single_file_tests(self, test_dir, test_file):
        desirable_working_directory = str(test_dir) + "/../"
        ret = subprocess.call(
            [sys.executable, self.test_runner_absolute_path, '--test-path',
             'tests/', '--test-pattern', test_file, self.google_cloud_sdk,
             '-v'],
            cwd=desirable_working_directory,
            stdout=self.std_output_and_error_redirect,
            stderr=self.std_output_and_error_redirect)
        if ret == 0:
            print "	OK", test_file
        else:
            self.errors_count += 1
            print "	ERROR", test_file


if __name__ == '__main__':
    IsolationTest.run(None, None, False)
