package cs4224;
import com.datastax.driver.core.*;

import java.math.BigDecimal;
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
        int inputWId = 1, inputCarrierId = 1;
        Delivery d = new Delivery(new SimpleClient());
        d.executeQuery(inputWId, inputCarrierId);
    }

    public Delivery(SimpleClient client) {
        this.session = client.getSession();

        this.selectMinOIdQuery = session.prepare("SELECT min(o_id) as min_o_id, o_w_id, o_d_id, o_c_id "
                + "FROM Orders where o_w_id = ? AND o_carrier_id = 0;");
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
        ResultSet results = session.execute(selectMinOIdQuery.bind(inputWId));
        int minOId = 0;
        int wId = 0, dId = 0, cId = 0;
        for (Row row : results) {
            minOId = row.getInt("min_o_id");
            wId = row.getInt("o_w_id");
            dId = row.getInt("o_d_id");
            cId = row.getInt("o_c_id");
            break;
        }

        session.execute(updateCarrierIdQuery.bind(inputCarrierId, wId, dId, minOId));

        Date now = new Date();

        results = session.execute(selectOlNumberQuery.bind(wId, dId, minOId));
        float olSum = 0;
        int olNumber = 0;
        for (Row row : results) {
            olSum += row.getDecimal("ol_amount").floatValue();
            olNumber = row.getInt("ol_number");
            session.execute(updateDeliveryDateQuery.bind(new Timestamp(now.getTime()), wId, dId, minOId, olNumber));
        }

        float cBalance = 0;
        int cCnt = 0;
        results = session.execute(selectBalanceCntQuery.bind(wId, dId, cId));
        for (Row row : results) {
            cBalance = row.getDecimal("c_balance").floatValue();
            cCnt = row.getInt("c_delivery_cnt");
        }
        cBalance += olSum;
        cCnt++;
        session.execute(updateBalanceCntQuery.bind(new BigDecimal(cBalance), cCnt, wId, dId, cId));
    }
}
