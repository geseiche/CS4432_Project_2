import simpledb.remote.SimpleDriver;

import java.sql.*;
import java.util.Random;

public class TestSmartMergeJoin {
    final static int maxSize=5000;

    public static void main(String[] args) {
        Connection conn=null;
        Driver d = new SimpleDriver();
        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
        String url = "jdbc:simpledb://" + host;
        Random rand=null;
        Statement s=null;
        try {
            conn = d.connect(url, null);
            s=conn.createStatement();

            //Create Tables
            s.executeUpdate("Create table test1" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test2" +
                    "( a2 int," +
                    "  a3 int"+
                    ")");

            //Populate Tables
            rand=new Random(1);// ensure every table gets the same data
            for(int j=0;j<maxSize;j++)
            {
                int a1 = rand.nextInt(100);
                int a2 = rand.nextInt(100);
                int a3 = rand.nextInt(100);

                if(j%3 != 0) {
                    s.executeUpdate("insert into test1 (a1,a2) values("+a1+","+a2+ ")");
                    s.executeUpdate("insert into test2 (a2,a3) values("+a2+","+a3+ ")");
                }
                else {
                    s.executeUpdate("insert into test1 (a1,a2) values("+a1+","+a2+ ")");
                    s.executeUpdate("insert into test2 (a2,a3) values("+a3+","+a2+ ")");
                }
            }

            System.out.println("Perform Query: 'select a1,a2 from test1'");
            ResultSet rs = s.executeQuery("select a1,a2 from test1");
//            while (rs.next()) {
//                int a1 = rs.getInt("a1");
//                int a2 = rs.getInt("a2");
//                System.out.println(a1 + "\t" + a2);
//            }
            rs.close();

            System.out.println("Perform Query: 'select a2,a3 from test2'");
            rs = s.executeQuery("select a2,a3 from test2");
//            while (rs.next()) {
//                int a2 = rs.getInt("a2");
//                int a3 = rs.getInt("a3");
//                System.out.println(a2 + "\t" + a3);
//            }
            rs.close();

            System.out.println("Perform First Join: 'select a1,a2,a3 from test1,test2'");
            long startTime = System.currentTimeMillis();
            rs = s.executeQuery("select a1,a2,a3 from test1,test2");
            long endTime = System.currentTimeMillis();
            System.out.println(String.format("Time to perform join: %d milliseconds", endTime - startTime));
//            while (rs.next()) {
//                int a1 = rs.getInt("a1");
//                int a2 = rs.getInt("a2");
//                int a3 = rs.getInt("a3");
//                System.out.println(a1 + "\t" + a2 + "\t" + a3);
//            }
            rs.close();

            System.out.println("Perform Second Join: 'select a1,a2,a3 from test1,test2'");
            startTime = System.currentTimeMillis();
            rs = s.executeQuery("select a1,a2,a3 from test1,test2");
            endTime = System.currentTimeMillis();
            System.out.println(String.format("Time to perform join: %d milliseconds", endTime - startTime));
//            while (rs.next()) {
//                int a1 = rs.getInt("a1");
//                int a2 = rs.getInt("a2");
//                int a3 = rs.getInt("a3");
//                System.out.println(a1 + "\t" + a2 + "\t" + a3);
//            }
            rs.close();

            conn.close();
        } catch (SQLException e) {
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
