import googleapiclient.discovery
import httplib2
from oauth2client.client import GoogleCredentials

from src.configuration import configuration

US_LOCATIONS = ["northamerica-northeast1",
                "us-central",
                "us-west2",
                "us-east1",
                "us-east4 ",
                "southamerica-east1"]
EU_LOCATIONS = ["europe-west",
                "europe-west2",
                "europe-west3"]
ASIA_LOCATIONS = ["asia-northeast1",
                  "asia-south1"]


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
        return self.__map(location_id)

    @staticmethod
    def __create_http():
        return httplib2.Http(timeout=60)

    @staticmethod
    def __map(location_id):
        if location_id in US_LOCATIONS:
            return "US"
        if location_id in EU_LOCATIONS:
            return "EU"
        if location_id in ASIA_LOCATIONS:
            return "Asia"
        return "UNKNOWN"
