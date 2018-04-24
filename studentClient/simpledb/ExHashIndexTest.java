import simpledb.remote.SimpleDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

public class ExHashIndexTest {
        public static void main(String[] args) {
        Connection conn = null;
        try {
            Driver d = new SimpleDriver();
            conn = d.connect("jdbc:simpledb://localhost", null);
            Statement stmt = conn.createStatement();

            String s = "create table numbers(a int, b int)";
            stmt.executeUpdate(s);
            System.out.println("Table NUMBERS created.");

            s = "create eh index ehnumbersa on numbers(a)";
            stmt.executeUpdate(s);
            System.out.println("ExHash Index created.");

            s = "insert into NUMBERS(a, b) values ";
            String[] numvals = {"(11, 6)",
                    "(3, 39)",
                    "(1, 18)",
                    "(8, 46)",
                    "(7, 4)",
                    "(2, 13)",
                    "(10, 19)",
                    "(4, 16)",
                    "(9, 31)",
                    "(6, 23)",
                    "(12, 40)",
                    "(5, 44)",
                    "(23, 6)",
                    "(15, 39)",
                    "(13, 18)",
                    "(20, 46)",
                    "(19, 4)",
                    "(14, 13)",
                    "(22, 19)",
                    "(16, 16)",
                    "(21, 31)",
                    "(18, 23)",
                    "(24, 40)",
                    "(17, 44)"};
            for (int i=0; i<numvals.length; i++)
                stmt.executeUpdate(s + numvals[i]);
            System.out.println("NUMBERS records inserted.");

        }
        catch(SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (conn != null)
                    conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
