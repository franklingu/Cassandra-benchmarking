package cs4224;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;

import java.util.List;


/**
 * Created by Wang Yu on 04-Oct-15.
 */
public class Payment {
    private PreparedStatement selectWarehouse;
    private PreparedStatement updateWarehouse;
    private PreparedStatement selectDistrict;
    private PreparedStatement updateDistrict;
    private PreparedStatement selectCustomer;
    private PreparedStatement updateCustomer;
    private Session session;

    public Payment(SimpleClient client){
        this.session = client.getSession();

        selectWarehouse = session.prepare("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM warehouses WHERE w_id = ?;");
        updateWarehouse = session.prepare("UPDATE warehouses SET w_ytd = ? WHERE w_id = ?;");

        selectDistrict = session.prepare("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM districts WHERE d_w_id = ? AND d_id = ?;");
        updateDistrict = session.prepare("UPDATE districts SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?;");

        selectCustomer = session.prepare("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit,"
                + "c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");
        updateCustomer = session.prepare("UPDATE customers SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");

    }

    public void processPayment(int C_W_ID, int C_D_ID, int C_ID, float PAYMENT){
        // update the warehouse C_W_ID by incrementing W_YTD by PAYMENT
        updateWarehouseYTD(C_W_ID, PAYMENT);
        // update the district (C_W_ID,C_D_ID) by incrementing D_YTD by PAYMENT
        updateDistrictYTD(C_W_ID, C_D_ID, PAYMENT);
        // update the customer (C_W_ID,C_D_ID,C_ID)
        updateCustomer(C_W_ID, C_D_ID, C_ID, PAYMENT);
    }

    private void updateWarehouseYTD(int w_id, float amount){
        ResultSet result = session.execute(selectWarehouse.bind(w_id));

        List<Row> warehouses = result.all();
        if( warehouses.isEmpty()){
            return;
        }
        Row warehouse = warehouses.get(0);
        float newYTD = warehouse.getFloat("w_ytd") + amount;

        session.execute(updateWarehouse.bind(newYTD, w_id));
        // display warehouse address
        System.out.println("Warehouse Address:");
        System.out.println(String.format("Street: %s %s, City: %s, State: %s, ZIP: %s", warehouse.getString("w_street_1"),
                warehouse.getString("w_street_2"), warehouse.getString("w_city"), warehouse.getString("w_state"), warehouse.getString("w_zip")));
    }

    private void updateDistrictYTD(int w_id, int d_id, float amount){
        ResultSet result = session.execute(selectDistrict.bind(w_id, d_id));

        List<Row> districts = result.all();
        if(districts.isEmpty()){
            return;
        }
        Row district = districts.get(0);
        float newYTD = district.getFloat("d_ytd") + amount;

        session.execute(updateDistrict.bind(newYTD, w_id, d_id));
        // display district address
        System.out.println("District Address:");
        System.out.println(String.format("Street: %s %s, City: %s, State: %s, ZIP: %s", district.getString("d_street_1"),
                district.getString("d_street_2"), district.getString("d_city"), district.getString("d_state"), district.getString("d_zip")));

    }

    private void updateCustomer(int w_id, int d_id, int c_id, float amount) {
        ResultSet result = session.execute(selectCustomer.bind(w_id, d_id, c_id));

        List<Row> customers = result.all();
        if(customers.isEmpty()){
            return;
        }
        Row customer = customers.get(0);
        float newBalance = customer.getFloat("c_balance") - amount;
        float newYTD = customer.getFloat("c_ytd_payment") + amount;
        int newCnt = customer.getInt("c_payment_cnt") + 1;

        session.execute(updateCustomer.bind(newBalance, newYTD, newCnt, w_id, d_id, c_id));
        // display district address
        System.out.println("Customer Information:");
        System.out.println(String.format("Customer ID: (%d,%d,%d), Name: (%s %s %s), Address: (%s,%s,%s,%s,%s)"
                        + "%s, %s, %s, %.2f, %.4f, %.2f", w_id, d_id, c_id, customer.getString("c_first"), customer.getString("c_middle"),
                        customer.getString("c_last"), customer.getString("c_street_1"), customer.getString("c_street_2"), customer.getString("c_city"),
                        customer.getString("c_state"), customer.getString("c_zip"), customer.getString("c_phone"), customer.getTimestamp("c_since"),
                        customer.getString("c_credit"), customer.getFloat("c_credit_lim"), customer.getFloat("c_discount"), customer.getFloat("c_balance")));
        System.out.println("Payment Amount:" + amount);
    }

}
