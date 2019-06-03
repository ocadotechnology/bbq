import webapp2
from google.appengine.datastore.datastore_query import Cursor

from src.retention.organization_retention import OrganizationRetention


class OrganizationRetentionHandler(webapp2.RequestHandler):

    def get(self):
        cursor = Cursor(urlsafe=self.request.get('cursor'))
        OrganizationRetention.schedule_retention_tasks_starting_from_cursor(cursor)
