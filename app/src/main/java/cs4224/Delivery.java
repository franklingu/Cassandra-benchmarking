package cs4224;
import com.datastax.driver.core.*;
import java.util.*;

/**
 * Created by junchao on 15-10-4.
 */
public class Delivery {
    public static void main(String[] args) {
        Cluster cluster;
        Session session;

        // Connect to the cluster and key space "cs4224"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("cs4224");

        int inputWId = 1, inputCarrierId = 1;

        String query = String.format("SELECT min(o_id) as min_o_id, o_w_id, o_d_id, o_c_id " +
                " FROM Orders where o_w_id = %d AND o_carrier_id = 0", inputWId);
        ResultSet results = session.execute(query);
        int minOId = 0;
        int wId = 0, dId = 0, cId = 0;
        for (Row row : results) {
            minOId = row.getInt("min_o_id");
            wId = row.getInt("o_w_id");
            dId = row.getInt("o_d_id");
            cId = row.getInt("o_c_id");
            System.out.format("%d %d %d %d\n", minOId, wId, dId, cId);
            break;
        }

        query = String.format("UPDATE Orders SET o_carrier_id = %d WHERE o_w_id = %d AND o_d_id = %d"
                + " AND o_id = %d", inputCarrierId, wId, dId, minOId);
        session.execute(query);

        Date now = new Date();

        query = String.format("SELECT ol_number, ol_amount FROM OrderLines WHERE ol_w_id = %d AND ol_d_id = %d"
                + " AND ol_o_id = %d", wId, dId, minOId);
        results = session.execute(query);
        float olSum = 0;
        int ol_number = 0;
        for (Row row : results) {
            olSum += row.getDecimal("ol_amount").floatValue();
            ol_number = row.getInt("ol_number");
            query = String.format("UPDATE OrderLines SET ol_delivery_d = %d WHERE ol_w_id = %d AND ol_d_id = %d"
                    + " AND ol_o_id = %d AND ol_number = %d", now.getTime(), wId, dId, minOId, ol_number);
            session.execute(query);
        }
        System.out.println(olSum);

        float cBalance = 0;
        int cCnt = 0;
        query = String.format("SELECT c_balance, c_delivery_cnt FROM Customers WHERE c_w_id = %d AND c_d_id = %d"
                 + " AND c_id = %d", wId, dId, cId);
        results = session.execute(query);
        for (Row row : results) {
            cBalance = row.getDecimal("c_balance").floatValue();
            cCnt = row.getInt("c_delivery_cnt");
        }
        System.out.println(cBalance);
        System.out.println(cCnt);
        cBalance += olSum;
        cCnt++;
        query = String.format("UPDATE Customers SET c_balance = %f WHERE c_w_id = %d AND c_d_id = %d"
                + " AND c_id = %d", cBalance, wId, dId, cId);
        session.execute(query);
        query = String.format("UPDATE Customers SET c_delivery_cnt = %d WHERE c_w_id = %d AND c_d_id = %d"
                + " AND c_id = %d", cCnt, wId, dId, cId);
        session.execute(query);

        // Clean up the connection by closing it
        cluster.close();
    }
}
