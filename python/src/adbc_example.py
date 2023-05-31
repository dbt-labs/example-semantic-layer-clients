import os
import sys
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect


@dataclass
class ConnAttr:
    host: str
    params: dict
    auth_header: str


def parse_jdbc_uri(uri):
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    return ConnAttr(
        host=parsed.path.replace("arrow-flight-sql", "grpc")
        if params.pop("useencryption") == "false"
        else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
        params=params,
        auth_header=f"Bearer {params.pop('token')}",
    )


def main(args):
    conn_attr = parse_jdbc_uri(os.environ["DBT_JDBC_URL"])
    with connect(
        conn_attr.host,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: conn_attr.auth_header,
            **{
                f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v
                for k, v in conn_attr.params.items()
            },
        },
    ) as conn, conn.cursor() as cur:
        cur.execute(args[1])
        df = cur.fetch_df()  # fetches as Pandas DF, can also do fetch_arrow_table
    print(df.to_string())


if __name__ == "__main__":
    main(sys.argv)
