package cs4224;

import com.datastax.driver.core.*;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by Jiang Sheng on 4/10/15.
 */
public class OrderStatus {
    private PreparedStatement customerQuery;
    private PreparedStatement orderLineQuery;
    private PreparedStatement orderQuery;
    private Session session;

    public OrderStatus(SimpleClient client) {
        session = client.getSession();

        this.customerQuery = session.prepare("select c_first, c_middle, c_last, c_balance from customers where c_w_id = ? and c_d_id = ? and c_id = ?;");
        this.orderQuery = session.prepare("select o_id, o_entry_d, o_carrier_id, o_ols from orders where o_w_id = ? and o_d_id = ? and o_c_id = ?;");
    }

    /**
     * Get Order Status data
     * @param c_w_id
     * @param c_d_id
     * @param c_id
     */
    public void getOrderStatus(int c_w_id, int c_d_id, int c_id) {
        // retrieve customer's information.
        ResultSet results = session.execute(customerQuery.bind(c_w_id, c_d_id, c_id));

        // should be a single value
        Row customer = results.all().get(0);
        System.out.println("Customer Info:");
        System.out.println(String.format("Name: %s %s %s ,Balance: %.4f", customer.getString("c_first"), customer.getString("c_middle"),
                customer.getString("c_last"), customer.getFloat("c_balance")));

        // retrieve order information for this customer
        results = session.execute(orderQuery.bind(c_w_id, c_d_id, c_id));
        List<Row> allOrders = results.all();
        if (allOrders.size() == 0) {
            return;
        }
        // take the largest value as last order
        int targetIndex = 0;
        int lastOrderId = allOrders.get(0).getInt("o_id");
        // keep index for target order in the list
        for (int i=0; i<allOrders.size();i++) {
            Row row = allOrders.get(i);
            if (row.getInt("o_id") > lastOrderId) {
                targetIndex = i;
            }
        }

        Row lastOrder = allOrders.get(targetIndex);
        int orderId = lastOrder.getInt("o_id");

        System.out.println("Last Order:");
        System.out.println(String.format("id: %d, time: %s, carrier_id: %d", orderId,
                lastOrder.getTimestamp("o_entry_d"), lastOrder.getInt("o_carrier_id")));

        // retrieve order-line for this order.
        Map<Integer, UDTValue> ols = lastOrder.getMap("o_ols", Integer.class, UDTValue.class);
        for (Integer key: ols.keySet()) {
            UDTValue ol = ols.get(key);
            System.out.println(String.format("%d, %d, %d, %.4f, %s ",ol.getInt("ol_i_id"), ol.getInt("ol_supply_w_id"),
                    ol.getInt("ol_quantity"), ol.getFloat("ol_amount"), ol.getTimestamp("ol_delivery_d")));
        }

        System.out.println();
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");

        OrderStatus transaction = new OrderStatus(client);
        transaction.getOrderStatus(1, 1, 1);
        transaction.getOrderStatus(3, 2, 20);
        transaction.getOrderStatus(2, 1, 1);

        client.close();
    }
}
