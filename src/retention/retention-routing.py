import webapp2

from src.environment import Environment
from src.retention.organization_retention_handler import \
    OrganizationRetentionHandler, OrganizationRetentionAuthenticatedHandler
from src.retention.table_retention_handler import TableRetentionHandler

app = webapp2.WSGIApplication([
    ('/cron/retention', OrganizationRetentionHandler),
    ('/retention/schedule', OrganizationRetentionAuthenticatedHandler),
    ('/tasks/retention/table', TableRetentionHandler)
], debug=Environment.is_debug_mode_allowed())
