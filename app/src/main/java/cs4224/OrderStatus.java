package cs4224;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

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

        this.customerQuery = session.prepare("select * from customers where c_w_id = ? and c_d_id = ? and c_id = ?;");
        this.orderLineQuery = session.prepare("select * from orderlines where ol_w_id = ? and ol_d_id = ? and ol_o_id = ?;");
        this.orderQuery = session.prepare("select * from orders where o_w_id = ? and o_d_id = ? and o_c_id = ?;");

        getOrderStatus(1, 1, 75);
        getOrderStatus(3, 8, 7);

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
                customer.getString("c_middle"), customer.getDecimal("c_balance")));

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
        results = session.execute(orderLineQuery.bind(c_w_id, c_d_id, orderId));
        System.out.println("Items in this order:");
        for (Row row : results) {
            System.out.println(String.format("%d, %d, %.4f, %.4f, %s ",row.getInt("ol_i_id"), row.getInt("ol_supply_w_id"),
                   row.getDecimal("ol_quantity"), row.getDecimal("ol_amount"), row.getTimestamp("ol_delivery_d")));
        }

        System.out.println();
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");

        OrderStatus transaction = new OrderStatus(client);
        transaction.getOrderStatus(1, 1, 5);
        transaction.getOrderStatus(3, 2, 20);
        transaction.getOrderStatus(2, 4, 10);

        client.close();
    }
}
