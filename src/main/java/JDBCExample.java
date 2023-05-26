import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;


class JDBCExample {
    public static void main(String[] args) {
        String connectionUrl = args[0];       
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            try (Statement s = conn.createStatement()) {
                ResultSet rs = s.executeQuery("select * from {{semantic_layer.query(metrics=['cancellation_rate'])}}");
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                   for (int i = 1; i <= columnsNumber; i++) {
                      if (i > 1) System.out.print(",  ");
                      String columnValue = rs.getString(i);
                      System.out.print(columnValue + " " + rsmd.getColumnName(i));
                   }
                   System.out.println("");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Finished");
    }
}
