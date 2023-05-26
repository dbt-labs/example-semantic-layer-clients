# Example for connecting to the JDBC driver from a Java project

This basic example shows how to set up a Java project using gradle to connect to and query the Semantic Layer.

To run this:

```
_JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED" ./gradlew run --args="<jdbc-url>"
```

NOTE: Arrow requires using the `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM option, otherwise you will get errors.
