import webapp2

from src.commons.config.configuration import configuration
from src.retention.organization_retention_handler import \
    OrganizationRetentionHandler
from src.retention.table_retention_handler import TableRetentionHandler

app = webapp2.WSGIApplication([
    ('/cron/retention', OrganizationRetentionHandler),
    ('/tasks/retention/table', TableRetentionHandler)
], debug=configuration.debug_mode)
