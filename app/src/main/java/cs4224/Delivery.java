package cs4224;
import com.datastax.driver.core.*;
import java.sql.Timestamp;
import java.util.*;

public class Delivery {
    private Session session;
    private PreparedStatement selectMinOIdQuery;
    private PreparedStatement updateCarrierIdQuery;
    private PreparedStatement selectOlNumberQuery;
    private PreparedStatement updateDeliveryDateQuery;
    private PreparedStatement selectBalanceCntQuery;
    private PreparedStatement updateBalanceCntQuery;


    public static void main(String[] args) {
        int inputinputWId = 1, inputCarrierId = 7;
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");
        Delivery d = new Delivery(client);
        d.executeQuery(inputinputWId, inputCarrierId);
        client.close();
    }

    public Delivery(SimpleClient client) {
        this.session = client.getSession();

        this.selectMinOIdQuery = session.prepare("SELECT min(o_id) as min_o_id, o_c_id "
                + "FROM Orders where o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0;");
        this.updateCarrierIdQuery = session.prepare("UPDATE Orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ?"
                + " AND o_id = ?;");
        this.selectOlNumberQuery = session.prepare("SELECT ol_number, ol_amount FROM OrderLines WHERE ol_w_id = ? AND ol_d_id = ?"
                + " AND ol_o_id = ?;");
        this.updateDeliveryDateQuery = session.prepare("UPDATE OrderLines SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ?"
                + " AND ol_o_id = ? AND ol_number = ?;");
        this.selectBalanceCntQuery = session.prepare("SELECT c_balance, c_delivery_cnt FROM Customers WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");
        this.updateBalanceCntQuery = session.prepare("UPDATE Customers SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");
    }

    public void executeQuery(int inputWId, int inputCarrierId) {
        for (int i = 1; i <= 10; i++) {
            int dId = i, cId = 0;
            ResultSet results = session.execute(selectMinOIdQuery.bind(inputWId, dId));
            int minOId = 0;
            for (Row row : results) {
                minOId = row.getInt("min_o_id");
                cId = row.getInt("o_c_id");
                break;
            }
            // System.out.format("MinOId: %d, CId: %d\n", minOId, cId);

            session.execute(updateCarrierIdQuery.bind(inputCarrierId, inputWId, dId, minOId));

            Date now = new Date();

            results = session.execute(selectOlNumberQuery.bind(inputWId, dId, minOId));
            float olSum = 0;
            int olNumber = 0;
            for (Row row : results) {
                olSum += row.getFloat("ol_amount");
                olNumber = row.getInt("ol_number");
                // System.out.format("OLNumber: %d\n", olNumber);
                session.execute(updateDeliveryDateQuery.bind(new Timestamp(now.getTime()), inputWId, dId, minOId, olNumber));
            }

            float cBalance = 0;
            int cCnt = 0;
            results = session.execute(selectBalanceCntQuery.bind(inputWId, dId, cId));
            for (Row row : results) {
                cBalance = row.getFloat("c_balance");
                cCnt = row.getInt("c_delivery_cnt");
            }
            // System.out.format("CBalance: %f, cCnt: %d\n", cBalance, cCnt);
            cBalance += olSum;
            cCnt++;
            session.execute(updateBalanceCntQuery.bind(cBalance, cCnt, inputWId, dId, cId));
        }
        System.out.format("Done with Delivery\n\n");
    }
}
