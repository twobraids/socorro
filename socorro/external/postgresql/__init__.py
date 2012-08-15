from configman import RequiredConfig, Namespace

required_config = Namespace()
required_config.add_option(
    name='database_host',
    default='localhost',
    doc='the hostname of the database',
)
required_config.add_option(
    name='database_name',
    default='breakpad',
    doc='the name of the database',
)
required_config.add_option(
    name='database_port',
    default=5432,
    doc='the port for the database',
)
required_config.add_option(
    name='database_user',
    default='breakpad_rw',
    doc='the name of the user within the database',
)
required_config.add_option(
    name='database_password',
    default='aPassword',
    doc="the user's database password",
)
