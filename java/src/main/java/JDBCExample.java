import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import org.verdictdb.commons.DBTablePrinter;

class JDBCExample {
    public static void main(String[] args) {
        String connectionUrl = System.getenv("DBT_JDBC_URL");
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            try (Statement s = conn.createStatement()) {
                try (ResultSet rs = s.executeQuery(args[0])) {
                    DBTablePrinter.printResultSet(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
