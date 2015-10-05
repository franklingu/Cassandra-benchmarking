package cs4224;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by Gison on 4/10/15.
 */
public class NewOrder {
    private PreparedStatement warehouseQuery;
    private PreparedStatement districtQuery;
    private PreparedStatement customerQuery;
    private PreparedStatement createOrderLineQuery;
    private PreparedStatement createOrderQuery;
    private Session session;

    public NewOrder(SimpleClient client) {
        session = client.getSession();

        this.warehouseQuery = session.prepare("select w_tax from warehouses where w_id = ?;");
        this.warehouseQuery = session.prepare("select c_last, c_credit, c_discount from customers where c_w_id = ? and c_d_id = ? and c_id = ?;");
        this.districtQuery = session.prepare("select d_next_o_id, d_tax from districts where d_w_id = ? and d_id = ?;");
        this.createOrderQuery = session.prepare("INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?);");
        this.createOrderLineQuery = session.prepare("INSERT INTO orderlines (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    }

    /**
     * Get Order Status data
     *
     * @param w_id
     * @param d_id
     * @param c_id
     */
    public void createOrder(int w_id, int d_id, int c_id, int num_items,
                            int[] item_number, int[] supplier_warehouse, int[] quantity) {

        // retrieve next available order number.
        ResultSet results = session.execute(districtQuery.bind(w_id, d_id));
        int orderNo = results.one().getInt("d_next_o_id");

        // district tax
        double district_tax = results.one().getInt("d_tax");

        // get warehouse tax rate
        results = session.execute(warehouseQuery.bind(w_id));
        double warehouse_tax = results.one().getDecimal("w_tax").doubleValue();

        // get customer info
        results = session.execute(customerQuery.bind(w_id, d_id, c_id));
        Row customer = results.one();
        double discount = customer.getDecimal("c_discount").doubleValue();
        String lastName = customer.getString("c_last");
        String credit = customer.getString("c_credit");

        System.out.println(String.format("%s, %s, %.2f", lastName, credit, discount));
        System.out.println(String.format("Warehouse Tax: %.2f, District tax: %.2f", warehouse_tax, district_tax));

        session.execute(String.format("update districts set d_next_o_id = %d where d_w_id = %d and d_id = %d;",
                orderNo + 1, w_id, d_id));

        // check if local order
        int isAllLocal = 1;
        for (int orderId : supplier_warehouse) {
            if (orderId != w_id) {
                isAllLocal = 0;
                break;
            }
        }

        // insert this order
        Date entryDate = new Date();
        session.execute(createOrderQuery.bind(w_id, d_id, orderNo, c_id, null, num_items, isAllLocal, entryDate));
        System.out.println(String.format("Order number: %d, %s", orderNo, entryDate));

        double totalAmount = 0.0;
        int item, warehouse, request_quantity;
        String district_info, name;
        double s_quantity, s_ytd, s_order_cnt, s_remote_cnt, adjusted_quantity;
        double price, item_amount;

        ArrayList<String> outputInfo = new ArrayList<String>();

        for (int i = 0; i < num_items; i++) {
            item = item_number[i];
            warehouse = supplier_warehouse[i];
            request_quantity = quantity[i];

            // get stock info
            results = session.execute(String.format("select s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_%02d from stocks where s_w_id = %d and s_i_id = %d;", w_id, item));
            s_quantity = results.one().getDecimal("s_quantity").doubleValue();
            s_ytd = results.one().getDecimal("s_ytd").doubleValue();
            s_order_cnt = results.one().getInt("s_order_cnt");
            s_remote_cnt = results.one().getInt("s_remote_cnt");
            district_info = results.one().getString(4);

            // adjust quantity
            adjusted_quantity = s_quantity - (float) request_quantity;
            adjusted_quantity = adjusted_quantity < 10 ? adjusted_quantity + 91 : adjusted_quantity;

            // check if it is a remote order
            s_remote_cnt = warehouse == w_id ? s_remote_cnt : s_remote_cnt + 1;

            // update stock
            session.execute(String.format("update stocks set s_quantity = %.2f, s_ytd = %.2f, s_order_cnt = %d, s_remote_cnt = %d where s_w_id = %d and s_i_id = %d;",
                    adjusted_quantity, s_ytd + request_quantity, s_order_cnt + 1, s_remote_cnt, w_id, item));

            // Get price for this item
            results = session.execute(String.format("select i_price, i_name from items where i_id = %d;", item));
            price = results.one().getDecimal("i_price").doubleValue();
            name = results.one().getString("i_name");
            // calculate amount for this item
            item_amount = request_quantity * price;
            // sum up total amount
            totalAmount += item_amount;

            // create new order line
            session.execute(createOrderLineQuery.bind(w_id, d_id, orderNo, i + 1, item, null, item_amount, warehouse, request_quantity, district_info));

            // add output info
            outputInfo.add(String.format("Item: %d: %s, Warehouse %d. Quantity: %d. Amount: %.2f. Stock: %.2f.", i + 1, name, warehouse, request_quantity, item_amount, s_quantity));
        }

        totalAmount = totalAmount * (1 + district_tax + warehouse_tax) * (1 - discount);
        System.out.println(String.format("Total items: %d, total amount: %.2f", num_items, totalAmount));

        for (String s : outputInfo) {
            System.out.println(s);
        }
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect("127.0.0.1", "cs4224");

        NewOrder transaction = new NewOrder(client);


        client.close();
    }


}
