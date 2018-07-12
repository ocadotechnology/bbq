import googleapiclient.discovery
import httplib2
from oauth2client.client import GoogleCredentials

from src.configuration import configuration


class AppInfo(object):

    def __init__(self):
        self.app_id = configuration.backup_project_id
        self.http = self.__create_http()
        self.service = googleapiclient.discovery.build(
            'appengine',
            'v1',
            credentials=GoogleCredentials.get_application_default(),
            http=self.http,
        )

    def get_location(self):
        app_info = self.service.apps().get(appsId=self.app_id).execute()
        location_id = app_info["locationId"]
        return "EU" if location_id.startswith("europe") else "US"

    @staticmethod
    def __create_http():
        return httplib2.Http(timeout=60)
