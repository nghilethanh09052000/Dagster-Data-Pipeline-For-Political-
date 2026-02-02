import dagster as dg
from dagster_slack import SlackResource
from psycopg_pool import ConnectionPool


class PostgresResource:
    """
    Singleton resources for all assets to use the Postgres connection pool
    """

    pool: ConnectionPool

    def __init__(self, conn_string: str) -> None:
        self.pool = ConnectionPool(
            conninfo=conn_string,
            timeout=3600 * 4,  # 4 hours
            # kwargs (dict) - Extra arguments to pass to connect(). Note that
            # this is one dict argument of the pool constructor, which is
            # expanded as connect() keyword parameters.
            kwargs={"autocommit": True, "keepalives_idle": 60},
        )


class ConfigurablePostgresResource(dg.ConfigurableResource):
    """
    Configuration for later creating the Postgres resources that could be used by any
    other assets in the project.
    """

    conn_string: str

    def create_resource(self, _: dg.InitResourceContext) -> PostgresResource:
        return PostgresResource(self.conn_string)


defs = dg.Definitions(
    resources={
        "pg": ConfigurablePostgresResource(
            conn_string=dg.EnvVar("PIPELINE_PG_CONNSTRING")
        ),
        "app_pg": ConfigurablePostgresResource(
            conn_string=dg.EnvVar("APPLICATION_PG_CONNSTRING")
        ),
        "slack": SlackResource(token=dg.EnvVar("SLACK_TOKEN")),
    }
)
