package cs4224;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;


/**
 * Created by Wang Yu on 04-Oct-15.
 */
public class Payment {
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final String DATABASE = "cs4224";

    private int C_W_ID;
    private int C_D_ID;
    private int C_ID;
    private double PAYMENT;

    public Payment(int c_w_id, int c_d_id,int c_id,double amount){
        this.C_W_ID = c_w_id;
        this.C_D_ID = c_d_id;
        this.C_ID = c_id;
        this.PAYMENT = amount;
    }

    public void processPayment(Session session){
        // update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
        updateWarehouseYTD(session, C_W_ID, PAYMENT);
        // update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
        updateDistrictYTD(session, C_W_ID, C_D_ID, PAYMENT);
        // update the customer (C_W_ID,C_D_ID,C_ID)
        updateCustomer(session, C_W_ID, C_D_ID, C_ID, PAYMENT);
    }

    private static void updateWarehouseYTD(Session session, int w_id, double amount){
        PreparedStatement selectWarehouse = session.prepare("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd"
                                                            + "FROM warehouses"
                                                            + "WHERE w_id = ?;");
        PreparedStatement updateWarehouse = session.prepare("UPDATE warehouses SET w_ytd = ? WHERE w_id = ?;");

        ResultSet result = session.execute(selectWarehouse.bind(w_id));

        if(result.all().size() == 0){
            return;
        }
        Row warehouse = result.all().get(0);
        double newYTD = warehouse.getDouble("w_ytd") + amount;

        session.execute(updateWarehouse.bind(newYTD, w_id));
        // display warehouse address
        System.out.println("Warehouse Address:");
        System.out.println(String.format("Street: %s %s, City: %s, State: %s, ZIP: %s", warehouse.getString("w_street_1"),
                warehouse.getString("w_street_2"), warehouse.getString("w_city"), warehouse.getString("w_state"), warehouse.getString("w_zip")));
    }

    private static void updateDistrictYTD(Session session, int w_id, int d_id, double amount){
        PreparedStatement selectDistrict = session.prepare("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd"
                                                            + "FROM districts WHERE d_w_id = ? AND d_id = ?;");
        PreparedStatement updateDistrict = session.prepare("UPDATE districts SET d_ytd = ?"
                                                            + "WHERE d_w_id = ? AND d_id = ?;");

        ResultSet result = session.execute(selectDistrict.bind(w_id, d_id));

        if(result.all().size() == 0){
            return;
        }
        Row district = result.all().get(0);
        double newYTD = district.getDouble("d_ytd") + amount;

        session.execute(updateDistrict.bind(newYTD, w_id, d_id));
        // display district address
        System.out.println("District Address:");
        System.out.println(String.format("Street: %s %s, City: %s, State: %s, ZIP: %s", district.getString("d_street_1"),
                district.getString("d_street_2"), district.getString("d_city"), district.getString("d_state"), district.getString("w_zip")));

    }

    private void updateCustomer(Session session, int w_id, int d_id, int c_id, double amount) {
        PreparedStatement selectCustomer = session.prepare("SELECT c_first, c_middle, c_last, c_street_1, c_street_2,"
                + "c_city,c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim"
                + "c_discount, c_balance, c_ydt_payment, c_payment_cnt"
                + "FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");
        PreparedStatement updateCustomer = session.prepare("UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?"
                + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");

        ResultSet result = session.execute(selectCustomer.bind(w_id, d_id, c_id));

        if(result.all().size() == 0){
            return;
        }
        Row customer = result.all().get(0);
        double newBalance = customer.getDouble("c_balance") - amount;
        double newYTD = customer.getDouble("c_ytd_payment") + amount;
        int newCnt = customer.getInt("c_payment_cnt") + 1;

        session.execute(updateCustomer.bind(newBalance, newYTD, newCnt, w_id, d_id, c_id));
        // display district address
        System.out.println("Customer Information:");
        System.out.println(String.format("Customer ID: (%d,%d,%d), Name: (%s %s %s), Address: (%s,%s,%s,%s,%s)"
                        + "%s, %s, %s, %.2f, %.4f, %.2f", w_id, d_id, c_id, customer.getString("c_first"), customer.getString("c_middle"),
                        customer.getString("c_last"), customer.getString("c_street_1"), customer.getString("c_street_2"), customer.getString("c_city"),
                        customer.getString("c_state"), customer.getString("c_zip"), customer.getString("c_phone"), customer.getTimestamp("c_since"),
                        customer.getString("c_credit"), customer.getDouble("c_credit_lim"), customer.getDouble("c_discount"), customer.getDouble("balance")));
        System.out.println("Payment Amount:" + amount);
    }

    public static void main(String[] args) {
        SimpleClient client = new SimpleClient();
        client.connect(LOCAL_HOST, DATABASE);
        Payment payment = new Payment(1,1,1,3.5);

        Session session = client.getSession();
        payment.processPayment(session);

        client.close();
    }
}
