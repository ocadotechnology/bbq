import webapp2

from src.commons.config.configuration import configuration
from src.retention.organization_retention_handler import \
    OrganizationRetentionHandler, OrganizationRetentionAuthenticatedHandler
from src.retention.table_retention_handler import TableRetentionHandler

app = webapp2.WSGIApplication([
    ('/cron/retention', OrganizationRetentionHandler),
    ('/retention/schedule', OrganizationRetentionAuthenticatedHandler),
    ('/tasks/retention/table', TableRetentionHandler)
], debug=configuration.debug_mode)
