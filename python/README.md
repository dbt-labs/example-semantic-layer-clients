# Example for connecting to the ADBC driver from a Python project

This basic example shows how to set up a Python project using hatch to connect to and query the Semantic Layer.

To run this, make sure you have set the `DBT_JDBC_URL` env var and run:

```
hatch run python src/adbc_example.py "<query>"
```
