package cs4224;

import com.datastax.driver.core.*;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Denormalization {
    private Session session;
    private PreparedStatement selectAllWIdsQuery;
    private PreparedStatement selectAllOrdersQuery;
    private PreparedStatement selectCorrespondingOrderLines;
    private PreparedStatement updateOrderForOrderLines;

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");
        Denormalization de = new Denormalization(client);
        de.setupDenormalization();
        client.close();
    }

    public Denormalization(SimpleClient client) {
        this.session = client.getSession();
        this.selectAllWIdsQuery = session.prepare("SELECT w_id FROM warehouses");
        this.selectAllOrdersQuery = session.prepare("SELECT o_id FROM orders WHERE o_w_id = ? AND o_d_id = ?");
        this.selectCorrespondingOrderLines = session.prepare("SELECT ol_number, ol_amount, ol_delivery_d," +
                " ol_dist_info, ol_i_id, ol_quantity, ol_supply_w_id FROM orderlines WHERE ol_w_id = ? AND ol_d_id = ?" +
                " AND ol_o_id = ?;");
        this.updateOrderForOrderLines = session.prepare("UPDATE orders set o_ols = ? where o_w_id = ?" +
                " and o_d_id = ? and o_id = ?;");
    }

    public void setupDenormalization() {
        ResultSet results = session.execute(selectAllWIdsQuery.bind());
        for (Row row: results) {
            int wId = row.getInt("w_id");
            for (int i = 1; i <= 10; i++) {
                int dId = i;
                ResultSet allOrdersResults = session.execute(selectAllOrdersQuery.bind(wId, dId));
                for (Row orderRow: allOrdersResults) {
                    int oId = orderRow.getInt("o_id");
                    Map<Integer, UDTValue> orderLines = new HashMap<Integer, UDTValue>();
                    UserType orderLineType = session.getCluster().getMetadata().getKeyspace("cs4224").getUserType("orderline");
                    UDTValue newOrderLine;
                    ResultSet allOrderLinesResults = session.execute(selectCorrespondingOrderLines.bind(wId, dId, oId));
                    for (Row orderLineRow: allOrderLinesResults) {
                        int olNumber = orderLineRow.getInt("ol_number");
                        float olAmount = orderLineRow.getFloat("ol_amount");
                        Date olDeliveryD = orderLineRow.getTimestamp("ol_delivery_d");
                        String olDistInfo = orderLineRow.getString("ol_dist_info");
                        int olIId = orderLineRow.getInt("ol_i_id");
                        int olQuantity = orderLineRow.getInt("ol_quantity");
                        int olSupplyWId = orderLineRow.getInt("ol_supply_w_id");
                        newOrderLine = orderLineType.newValue()
                                .setInt("OL_I_ID", olIId)
                                .setTimestamp("ol_delivery_d", olDeliveryD == null ? null : (new Timestamp(olDeliveryD.getTime())))
                                .setFloat("ol_amount", olAmount)
                                .setInt("ol_supply_w_id", olSupplyWId)
                                .setInt("ol_quantity", olQuantity)
                                .setString("ol_dist_info", olDistInfo);
                        orderLines.put(olNumber, newOrderLine);
                    }
                    session.execute(updateOrderForOrderLines.bind(orderLines, wId, dId, oId));
                }
            }
        }
        System.out.println("Done with denormalization");
    }
}
