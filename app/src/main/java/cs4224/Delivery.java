package cs4224;
import com.datastax.driver.core.*;

/**
 * Created by junchao on 15-10-4.
 */
public class Delivery {
    public static void main(String[] args) {
        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "cs4224"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("cs4224");

        // Use select to get the user we just entered
        String query = "SELECT min(o_id) FROM orders where o_w_id = " + 1 + " AND o_carrier_id = null";
        ResultSet results = session.execute(query);
        for (Row row : results) {
            System.out.format("%d %s\n", row.getInt("o_id"), row.getString("w_city"));
        }

        // Clean up the connection by closing it
        cluster.close();
    }
}
