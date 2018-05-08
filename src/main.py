import webapp2

from src.environment import Environment


class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.write(
            'This is <strong>Backup BigQuery</strong> (BBQ) project.<br>'
        )

        self.response.write(
            'This is {} environment.'.format(Environment.get_name())
        )


app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/_ah/start', MainPage)
], debug=Environment.is_debug_mode_allowed())
