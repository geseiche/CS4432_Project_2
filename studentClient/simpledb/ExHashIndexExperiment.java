import simpledb.index.hash.HashIndex;
import simpledb.remote.SimpleDriver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

//CS4432: Added a class for the querying of the tables with various types of indexes to test the speed
public class ExHashIndexExperiment {

    public static void main (String [] args){

        Connection conn=null;
        Driver d = new SimpleDriver();
        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
        String url = "jdbc:simpledb://" + host;
        Statement s = null;

        try {
            BufferedWriter file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt"));
            conn = d.connect(url, null);
            s=conn.createStatement();

            //TODO insert print statements in the query plans for I/Os
            file.append("No Index:\n");
            file.close();
            long startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test1 where a1 = 100");
            long endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Static Hash Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test2 where a1 = 100");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Extensible Hash Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test3 where a1 = 100");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Binary Tree Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test4 where a1 = 100");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            //TODO insert print statements in the query plans for I/Os
            file.append("Join with No Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test5, test1 where a1 = a1");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Join with Static Hash Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test5, test2 where a1 = a1");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Join with Extensible Hash Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test5, test3 where a1 = a1");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));

            file.append("Join with Binary Tree Index:\n");
            file.close();
            startTime = System.currentTimeMillis();
            s.executeQuery("select a1, a2 from test5, test4 where a1 = a1");
            endTime = System.currentTimeMillis();
            file = new BufferedWriter(new FileWriter("IndexExperimentResults.txt", true));
            file.append(String.format("\tTime: %d milliseconds\n", endTime - startTime));


            file.close();
        } catch (IOException|SQLException e){
            e.printStackTrace();
        }

    }
}
