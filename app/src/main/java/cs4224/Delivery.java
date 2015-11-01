package cs4224;
import com.datastax.driver.core.*;

import java.sql.Timestamp;
import java.util.*;

public class Delivery {
    private Session session;
    private PreparedStatement selectMinOIdQuery;
    private PreparedStatement updateDeliveryDateQuery;
    private PreparedStatement selectBalanceCntQuery;
    private PreparedStatement updateBalanceCntQuery;


    public static void main(String[] args) {
        int inputWId = 1, inputCarrierId = 7;
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");
        Delivery d = new Delivery(client);
        d.executeQuery(inputWId, inputCarrierId);
        client.close();
    }

    public Delivery(SimpleClient client) {
        this.session = client.getSession();

        this.selectMinOIdQuery = session.prepare("SELECT min(o_id) as min_o_id, o_c_id, o_ols "
                + "FROM Orders where o_w_id = ? AND o_d_id = ? AND o_carrier_id = 0;");
        this.updateDeliveryDateQuery = session.prepare("UPDATE Orders set o_carrier_id = ?, o_ols = ? where o_w_id = ?" +
                " and o_d_id = ? and o_id = ?;");
        this.selectBalanceCntQuery = session.prepare("SELECT c_balance, c_delivery_cnt FROM Customers WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");
        this.updateBalanceCntQuery = session.prepare("UPDATE Customers SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");
    }

    public void executeQuery(int inputWId, int inputCarrierId) {
        for (int i = 1; i <= 10; i++) {
            int dId = i;
            int minOId = 0, cId = 0;
            float olSum = 0;
            int olQuantity, olSupplyWId, olIId;
            float olAmount;
            String olDistInfo;
            ResultSet results = session.execute(selectMinOIdQuery.bind(inputWId, dId));
            Date now = new Date();
            Map<Integer, UDTValue> orderLines = new HashMap<Integer, UDTValue>();
            UserType orderLineType = session.getCluster().getMetadata().getKeyspace("cs4224").getUserType("orderline");
            UDTValue newOrderLine;
            for (Row row: results) {
                minOId = row.getInt("min_o_id");
                System.out.format("%d\n", minOId);
                cId = row.getInt("o_c_id");
                Map<Integer, UDTValue> ols = row.getMap("o_ols", Integer.class, UDTValue.class);
                for (Integer key: ols.keySet()) {
                    UDTValue ol = ols.get(key);
                    olIId = ol.getInt("ol_i_id");
                    olQuantity = ol.getInt("ol_quantity");
                    olDistInfo = ol.getString("ol_dist_info");
                    olSupplyWId = ol.getInt("ol_supply_w_id");
                    olAmount = ol.getFloat("ol_amount");
                    olSum += olAmount;
                    newOrderLine = orderLineType.newValue()
                            .setInt("OL_I_ID", olIId)
                            .setTimestamp("ol_delivery_d", new Timestamp(now.getTime()))
                            .setFloat("ol_amount", olAmount)
                            .setInt("ol_supply_w_id", olSupplyWId)
                            .setInt("ol_quantity", olQuantity)
                            .setString("ol_dist_info", olDistInfo);
                    orderLines.put(key, newOrderLine);
                }
                session.execute(updateDeliveryDateQuery.bind(inputCarrierId, orderLines, inputWId, dId, minOId));
                break;
            }

            float cBalance = 0;
            int cCnt = 0;
            results = session.execute(selectBalanceCntQuery.bind(inputWId, dId, cId));
            for (Row row : results) {
                cBalance = row.getFloat("c_balance");
                cCnt = row.getInt("c_delivery_cnt");
            }
            cBalance += olSum;
            cCnt++;
            session.execute(updateBalanceCntQuery.bind(cBalance, cCnt, inputWId, dId, cId));
        }
        System.out.format("Done with Delivery\n\n");
    }
}
