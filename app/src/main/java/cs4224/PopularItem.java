package cs4224;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.HashMap;

/**
 * Created by Wang Yu on 05-Oct-15.
 */
public class PopularItem {
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String DATABASE = "cs4224";

    private int W_ID;
    private int D_ID;
    private int range;

    public PopularItem(int w_id, int d_id, int range){
        this.W_ID = w_id;
        this.D_ID = d_id;
        this.range = range;
    }

    public void findItem(Session session){
        // find next available order id in the district (D_W_ID,D_ID)
        int nextOrderID = findNextOrderID(session, W_ID, D_ID);
        // select orders with o_id > nextOrderID - range
        PreparedStatement selectOrder = session.prepare("SELECT o_id, o_c_id, o_entry_d FROM orders"
                + "WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ?;");
        ResultSet orders = session.execute(selectOrder.bind(W_ID, D_ID, nextOrderID - range));

        HashMap<Integer, Integer> itemCount = new HashMap<Integer, Integer>();
        HashMap<Integer, String> popularItms = new HashMap<Integer, String>();

        // process each order and find most popular item for each order
        for(Row order: orders){
            int orderID = order.getInt("o_id");
            int customerID = order.getInt("o_c_id");

            // find customer whom placed this order
            PreparedStatement selectCustomer = session.prepare("SELECT c_first, c_middle, c_last FROM customers"
                    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");
            ResultSet customers = session.execute(selectCustomer.bind(W_ID, D_ID, customerID));
            Row customer = customers.all().get(0);

            System.out.println(String.format("Order Number: %d, Entry Date and Time: %s, Name: (%s %s %s),", orderID, order.getTimestamp("o_entry_d"),
                    customer.getString("c_first"), customer.getString("c_middle"), customer.getString("c_last")));

            // find order details from orderlines
            PreparedStatement selectOrderLine = session.prepare("SELECT ol_i_id, ol_quantity FROM orderlines"
                    + "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;");
            ResultSet orderLines = session.execute(selectOrderLine.bind(W_ID, D_ID, orderID));

            int popularItmID = -1;
            int popularItmCnt = 0;
            for(Row orderLine: orderLines){
                int quantity = orderLine.getInt("ol_quantity");
                int itmID = orderLine.getInt("ol_i_id");
                if( quantity > popularItmCnt ){
                    popularItmCnt = quantity;
                    popularItmID = itmID;
                }
                if(!itemCount.containsKey(itmID)){
                    itemCount.put(itmID, 1);
                }else{
                    itemCount.put(itmID, itemCount.get(itmID) + 1);
                }
            }

            // find popular item name from items
            PreparedStatement selectItem = session.prepare("SELECT i_name FROM items WHERE i_id = ?;");
            ResultSet popularItems = session.execute(selectItem.bind(popularItmID));
            Row popularItem = popularItems.all().get(0);
            String itmName = popularItem.getString("i_name");
            System.out.println(String.format("Item Name: %s, Quantity: %d", itmName, popularItmCnt));

            // if not existing popular items add to them
            if(!popularItms.containsKey(popularItmID)){
                popularItms.put(popularItmID,itmName);
            }
        }

        for(Integer itmID : popularItms.keySet()){
            System.out.print("Item:" + popularItms.get(itmID) + itemCount.get(itmID));
        }
    }

    private int findNextOrderID(Session session, int w_id, int d_id) {
        PreparedStatement selectDistrict = session.prepare("SELECT d_next_o_id FROM districts"
                                                            + "WHERE d_w_id = ? AND d_id = ?;");

        ResultSet result = session.execute(selectDistrict.bind(w_id, d_id));

        if(result.all().size() == 0){
            return -1;
        }
        Row district = result.all().get(0);
        return district.getInt("d_next_o_id");
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect(LOCAL_HOST, DATABASE);
        PopularItem popularItem = new PopularItem(1,1,5);

        Session session = client.getSession();
        popularItem.findItem(session);

        client.close();
    }
}