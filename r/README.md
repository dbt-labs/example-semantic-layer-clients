# Example for connecting to the ADBC driver from a R script

This basic example shows how to connect to and query the Semantic Layer using the ADBC in R scripts.

You will need to first install the `arrow` and `adbcflightsql` packages (see the commented out lines at the top of the R script).

To run this, make sure you have set the `DBT_JDBC_URL` env var and run:

```
Rscript adbc_example.R "select * from {{ semantic_layer.query(metrics=['order_total'], group_by=['metric_time']) }}"
```
