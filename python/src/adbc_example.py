"""
This is an example for how to query the Semantic Layer using the ADBC driver.

This example is meant to be run from the CLI when you have the env var DBT_JDBC_URL
set. See the README for details.

For using in a Jupyter notebook or your own program, you should only need the `execute_query`
function, and you need to adapt it to your needs. In this example we simply print out the
DataFrame, but you probably want to return it instead.
"""
import os
import sys
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    environment_id: str  # 42
    token: str  # dbts_thisismyprivateservicetoken


def parse_jdbc_uri(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    return ConnAttr(
        host=parsed.path.replace("arrow-flight-sql", "grpc")
        if params.pop("useencryption", None) == "false"
        else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
        environment_id=params.pop('environmentid'),
        token=params.pop('token'),
    )


def execute_query(host, environment_id, token, query, db_opts=None):
    """Execute a Semantic Layer query.

    host must be in a fromat like: grpc+tls:semantic-layer.cloud.getdbt.com:443
    db_opts is a dictionary of additional DB options to pass in
    """
    opts = db_opts or {}

    with connect(
        conn_attr.host,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: f"Bearer {token}",
            f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}environmentid": environment_id,
            DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true",
            **opts
        },
    ) as conn, conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_df()  # fetches as Pandas DF, can also do fetch_arrow_table
    print(df.to_string())


if __name__ == "__main__":
    query = sys.argv[1]
    conn_attr = parse_jdbc_uri(os.environ["DBT_JDBC_URL"])

    execute_query(conn_attr.host, conn_attr.environment_id, conn_attr.token, query)
