//Use the following java code to create data.
/******************************************************************/
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
public class CreateTestTables {
    final static int maxSize=1000;
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn=null;
        Driver d = new SimpleDriver();
        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
        String url = "jdbc:simpledb://" + host;
        String qry="Create table test1" +
                "( a1 int," +
                "  a2 int"+
                ")";
        Random rand=null;
        Statement s=null;
        try {
            conn = d.connect(url, null);
            s=conn.createStatement();
            s.executeUpdate("Create table test1" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test2" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test3" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test4" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test5" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");

            s.executeUpdate("create sh index idx2 on test2 (a1)");
            s.executeUpdate("create eh index idx3 on test3 (a1)");
            s.executeUpdate("create bt index idx4 on test4 (a1)");
            for(int i=1;i<6;i++)
            {
                if(i!=5)
                {
                    rand=new Random(1);// ensure every table gets the same data
                    for(int j=0;j<maxSize;j++)
                    {
                        s.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
                    }
                }
                else//case where i=5
                {
                    for(int j=0;j<maxSize/2;j++)// insert 10000 records into test5
                    {
                        s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
                    }
                }
            }

            conn.close();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally
        {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

