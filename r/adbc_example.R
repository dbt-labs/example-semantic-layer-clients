# This is an example for how to query the Semantic Layer using the ADBC driver.
#
# This example is meant to be run from the CLI when you have the env var DBT_JDBC_URL
# set. See the README for details.
#
# You will need to install the arrow package and the adbcflightsql package by uncommenting
# the lines below before this example will work.
#
# You will likely need to adapt this for your own usage.

# install.packages("arrow")
# install.packages("pak")
# pak::pak("apache/arrow-adbc/r/adbcflightsql")

library(adbcdrivermanager)
library(adbcflightsql)
library(arrow)
library(httr2)

query <- commandArgs(trailingOnly=TRUE)[1] # "select * from {{ semantic_layer.query(metrics=['order_total'], group_by=['metric_time']) }}"

parsed_jdbc_url <- url_parse(Sys.getenv("DBT_JDBC_URL"))

host <- gsub("arrow-flight-sql", "grpc+tls", parsed_jdbc_url$path) # "grpc+tls:semantic-layer.cloud.getdbt.com:443"
environmentId <- parsed_jdbc_url$query$environmentId # "42"
token <- parsed_jdbc_url$query$token # "dbts_thisismyprivateservicetoken"

db <- adbcdrivermanager::adbc_database_init(
  adbcflightsql::adbcflightsql(),
  uri = host,
  adbc.flight.sql.authorization_header = paste("Bearer ", token),
  adbc.flight.sql.rpc.call_header.environmentid = environmentId,
  adbc.flight.sql.rpc.with_cookie_middleware = "true"
)
con <- adbcdrivermanager::adbc_connection_init(db)

df <- con |>
  read_adbc(query) |>
  tibble::as_tibble()

print(df)

adbc_connection_release(con)
adbc_database_release(db)
