import json
import os
import sys
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse
from typing import Dict
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
        if params.pop("useencryption", "true") == "false"
        else parsed.path.replace("arrow-flight-sql", "grpc+tls"),
        params=params,
        auth_header=f"Bearer {params.pop('token')}",
    )

# TODO: fix
def main(args):
    conn_attr = parse_jdbc_uri(os.environ["DBT_JDBC_URL"])
    with connect(
        conn_attr.host,
        db_kwargs={
            DatabaseOptions.AUTHORIZATION_HEADER.value: conn_attr.auth_header,
            DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true",
            **{
                f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v
                for k, v in conn_attr.params.items()
            },
        },
    ) as conn, conn.cursor() as cur:
        errors = {}
        success = []
        metrics = get_metrics(cur)
        for metric, properties in metrics.items():
            try:
                cur.execute(
                    f"select * from {{{{semantic_layer.query(metrics=['{metric}'])}}}}"
                )
                cur.fetch_df()
                success.append(metric)
            except Exception as e:
                errors[metric] = str(e)

        print(json.dumps(errors, indent=2).replace("\\n", "\n"))

        print(f"Successful metrics: {success}")


def get_metrics(cur) -> Dict[str, Dict[str, str]]:
    cur.execute("select * from {{semantic_layer.metrics()}}")
    df = cur.fetch_df()
    return {
        r["NAME"]: {"dimensions": set(r["DIMENSIONS"].split(", ")), "type": r["TYPE"]}
        for i, r in df.iterrows()
    }


if __name__ == "__main__":
    main(sys.argv)
