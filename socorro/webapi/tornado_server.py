from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

from socorro.webapi.servers import ApacheModWSGI
from configman import Namespace

#==============================================================================
class Tornado(ApacheModWSGI):
    required_config = Namespace()
    required_config.add_option(
      'port',
      doc='the port to listen to for submissions',
      default=8882
    )

    #--------------------------------------------------------------------------
    def run(self):
        wsgi_func = super(Tornado, self).run()

        container = WSGIContainer(wsgi_func)
        http_server = HTTPServer(container)
        http_server.listen(self.config.web_server.port)
        IOLoop.instance().start()

    #--------------------------------------------------------------------------
    def _identify(self):
        self.config.logger.info('this is the Tornado Web Server')

