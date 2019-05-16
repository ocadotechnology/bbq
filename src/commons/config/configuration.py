import logging

import yaml


class Configuration(object):

    def __init__(self, project_config_filename):
        self.__project_config = self.__load_yaml_config(project_config_filename)

    @staticmethod
    def __load_yaml_config(config_file):
        with open(config_file, 'r') as stream:
            try:
                return yaml.load(stream)
            except yaml.YAMLError as exc:
                logging.error(
                    'Unable to read \'%s\' config file configuration: %s',
                    config_file, exc)

    @property
    def debug_mode(self):
        return self.__project_config['project_settings'].get('debug_mode')

    @property
    def copy_job_result_check_countdown_in_sec(self):
        return self.__project_config['copy_jobs'].get('copy_job_result_check_countdown_in_sec')

    @property
    def backup_worker_max_countdown_in_sec(self):
        return self.__project_config['backup_settings'].get('backup_worker_max_countdown_in_sec')

    @property
    def backup_settings_custom_project_list(self):
        return self.__project_config['backup_settings'].get('custom_project_list')

    @property
    def backup_project_id(self):
        return self.__project_config['project_settings'].get('backup_project_id')

    @property
    def metadata_storage_project_id(self):
        return self.__project_config['project_settings'].get('metadata_storage_project_id')

    @property
    def default_restoration_project_id(self):
        return self.__project_config['project_settings'].get('default_restoration_project_id', '')

    @property
    def projects_to_skip(self):
        return self.__project_config['backup_settings'].get('projects_to_skip')


config_file_yaml = "config/config.yaml"
logging.info("Loading configuration from file: '%s'", config_file_yaml)
configuration = Configuration(config_file_yaml)
