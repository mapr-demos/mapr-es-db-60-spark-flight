package maprdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Sample JDBC based application to obtain details of all businesses that have
 * at least 100 reviews and a rating greater than 3.
 */
public class DRILL_SimpleQuery {

    public static String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";

    /**
     * Can specify connection URL in 2 ways. 1. Connect to Zookeeper -
     * "jdbc:drill:zk=<hostname/host-ip>:5181/drill/<cluster-name>-drillbits" 2.
     * Connect to Drillbit - "jdbc:drill:drillbit=<hostname>"
     * jdbc:drill:zk=maprdemo:5181/drill/maprdemo.mapr.io-drillbits
     * "jdbc:drill:drillbit=localhost";
     */
    private static String DRILL_JDBC_URL = "jdbc:drill:drillbit=localhost";

    public static void main(String[] args) {
        String tableName = "/mapr/maprdemo.mapr.io/apps/flights";
        if (args.length == 1) {
            tableName = args[0];

        } else {
            System.out.println("Using hard coded parameters unless you specify the file and topic. <file topic>   ");
        }

        try {
            Class.forName(JDBC_DRIVER);
            //Username and password have to be provided to obtain connection.
            //Ensure that the user provided is present in the cluster / sandbox
            Connection connection = DriverManager.getConnection(DRILL_JDBC_URL, "mapr", "");

            Statement statement = connection.createStatement();
            System.out.println(" 10 ids");
            final String sql = "select origin, pred_dtree, count(pred_dtree) as countp from dfs.`" + tableName + "`  group by origin, pred_dtree order by origin";
            System.out.println("Query: " + sql);

            ResultSet result = statement.executeQuery(sql);

            while (result.next()) {
                System.out.println("{\"origin\": \"" + result.getString(1) + "\", "
                        + "\"prediction\": " + result.getString(2) + "\", "
                        + "\"prediction count\": " + result.getString(3) + "}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
