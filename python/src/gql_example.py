import base64
import os
import string
import sys
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow as pa

s = httpx.Client(http2=True)


@dataclass
class ConnAttr:
    host: str  # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
    params: dict  # {"environmentId": 42}
    token: str  # "dbts_thisismyprivateservicetoken"


def parse_jdbc_uri(uri):
    """Helper function to convert the JDBC url into ConnAttr."""
    parsed = urlparse(uri)
    params = {k.lower(): v[0] for k, v in parse_qs(parsed.query).items()}
    return ConnAttr(
        host=parsed.path.replace("arrow-flight-sql", "https"),
        params=params,
        token=params.pop("token"),
    )


def main(args):
    conn_attr = parse_jdbc_uri(os.environ["DBT_JDBC_URL"])
    api_host = conn_attr.host.strip(":443") + "/api/graphql"
    env_id = conn_attr.params["environmentid"]
    make_request(env_id, api_host, conn_attr.token)


def make_request(env_id, api_host, token):
    headers = {"authorization": f"Bearer {token}"}
    mut = string.Template(
        """
        mutation {
          createQuery(environmentId:$environment_id, metrics:[{name:"order_total"}], groupBy:[{name:"metric_time"}]){
            queryId
          }
        }
        """
    ).substitute(environment_id=env_id)

    resp = s.post(api_host, json={"query": mut}, headers=headers)
    query_id = resp.json()["data"]["createQuery"]["queryId"]
    query = string.Template(
        """
        {
          query(queryId:"$query_id", environmentId:$environment_id){
            status
            arrowResult
            error
            queryId
          }
        }
        """
    ).substitute(query_id=query_id, environment_id=env_id)
    resp = s.post(api_host, json={"query": query}, headers=headers)
    while resp and resp.json()["data"]["query"]["status"] not in [
        "SUCCESSFUL",
        "FAILED",
    ]:
        resp = s.post(api_host, json={"query": query}, headers=headers)
    # TODO: handle pagination
    with pa.ipc.open_stream(
        base64.b64decode(resp.json()["data"]["query"]["arrowResult"])
    ) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)
    print(arrow_table.to_pandas())


if __name__ == "__main__":
    main(sys.argv)
