import os
from socorro.app.generic_app import main
from socorro.middleware.middleware_app import MiddlewareApp
from socorro.webapi.servers import ApacheModWSGI
from socorro.middleware.middleware_app import (
    application,
    lower_environment
)
import socorro.middleware.middleware_app

from configman import (
    ConfigFileFutureProxy,
    environment
)

if os.path.isfile('/etc/socorro/middleware.ini'):
    config_path = '/etc/socorro'
else:
    config_path = ApacheModWSGI.get_socorro_config_path(__file__)

# invoke the generic main function to create the configman app class and which
# will then create the wsgi app object.
main(
    MiddlewareApp,  # the socorro app class
    config_path=config_path,
    values_source_list=[
        ConfigFileFutureProxy,
        lower_environment
    ]
)
