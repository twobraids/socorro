import web

from socorro.webapi.classPartial import classWithPartialInit

from configman import Namespace, RequiredConfig

#==============================================================================
class WebServerBase(RequiredConfig):
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config, services_list):
        self.config = config
        self.urls = tuple(y for aTuple in
                         ((x.uri, classWithPartialInit(x, config))
                            for x in services_list) for y in aTuple)
        self.config.logger.info(str(self.urls))
        web.webapi.internalerror = web.debugerror
        web.config.debug = False
        self._identify()

    #--------------------------------------------------------------------------
    def _identify(self):
        pass



#==============================================================================
class ApacheModWSGI(WebServerBase):

    #--------------------------------------------------------------------------
    def run(self):
        return web.application(self.urls, globals()).wsgifunc()

    #--------------------------------------------------------------------------
    def _identify(self):
        self.config.logger.info('this is ApacheModWSGI')


#==============================================================================
class CherryPyStandAloneApplication(web.application):
    """When running a standalone web app based on web.py, the web.py code
    unfortunately makes some assumptions about the use of command line
    parameters for setting the host and port names.  This wrapper class
    eliminates that ambiguity by overriding the run method to use the host and
    port assigned in the constructor."""

    #--------------------------------------------------------------------------
    def __init__(self, server_ip_address, server_port, *args, **kwargs):
        """Construct the web application."""
        self.serverIpAddress = server_ip_address
        self.serverPort = server_port
        web.application.__init__(self, *args, **kwargs)

    #--------------------------------------------------------------------------
    def run(self, *additional_params):
        """Run the application."""
        f = self.wsgifunc(*additional_params)
        web.runsimple(f, (self.serverIpAddress, self.serverPort))


#==============================================================================
class StandAloneServer(WebServerBase):
    required_config = Namespace()
    required_config.add_option(
      'port',
      doc='the port to listen to for submissions',
      default=8882
    )
    required_config.add_option(
      'ip_address',
      doc='the IP address from which to accept submissions',
      default='127.0.0.1'
    )


#==============================================================================
class CherryPy(StandAloneServer):

    #--------------------------------------------------------------------------
    def run(self):
        app = CherryPyStandAloneApplication(
          self.config.web_server.ip_address,
          self.config.web_server.port,
          self.urls,
          globals()
        )
        app.run()

    #--------------------------------------------------------------------------
    def _identify(self):
        self.config.logger.info(
          'this is CherryPy from web.py running standalone at %s:%d',
          self.config.web_server.ip_address,
          self.config.web_server.port
        )


