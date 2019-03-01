import googleapiclient.discovery
from oauth2client.client import GoogleCredentials

from src.commons.config.configuration import configuration


class AppInfo(object):

    def __init__(self):
        self.app_id = configuration.backup_project_id
        self.service = googleapiclient.discovery.build(
            'appengine',
            'v1',
            credentials=self._create_credentials(),
            http=self._create_http(),
        )

    def get_location(self):
        app_info = self.service.apps().get(appsId=self.app_id).execute()
        location_id = app_info["locationId"]
        return "EU" if location_id.startswith("europe") else "US"

    @staticmethod
    def _create_credentials():
        return GoogleCredentials.get_application_default()

    @staticmethod
    def _create_http():
        return None
