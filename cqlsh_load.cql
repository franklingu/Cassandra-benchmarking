use cs4224;

-- sed -i 's/,null,/,0,/' order.csv 

copy customers (c_w_id, c_d_id, c_id, c_first, c_middle, c_last,c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) from 'customer.csv';

copy orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) from 'order.csv';

copy orderlines (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) from 'order-line.csv';

copy stocks (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) from 'stock.csv';

exit;
