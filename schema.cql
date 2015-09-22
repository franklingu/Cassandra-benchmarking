-- CS4224 Project
-- Sep 19, 2015

-- Drop existing namespace
DROP KEYSPACE IF EXISTS cs4224;

-- Create Namespace
CREATE KEYSPACE cs4224 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use cs4224;

-- Create table
CREATE TABLE Warehouses (
	W_ID int,
	W_NAME varchar,
	W_STREET_1 varchar,
	W_STREET_2 varchar,
	W_CITY varchar,
	W_STATE varchar,
	W_ZIP varchar,
	W_TAX decimal,
	W_YTD decimal,
	PRIMARY KEY (W_ID)
);

CREATE TABLE Districts (
	D_W_ID int,
	D_ID int,
	D_NAME varchar,
	D_STREET_1 varchar,
	D_STREET_2 varchar,
	D_CITY varchar,
	D_STATE varchar,
	D_ZIP varchar,
	D_TAX decimal,
	D_YTD decimal,
	D_NEXT_O_ID int,
	PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE Customers (
	C_W_ID int,
	C_D_ID int,
	C_ID int,
	C_FIRST varchar,
	C_MIDDLE varchar,
	C_LAST varchar,
	C_STREET_1 varchar,
	C_STREET_2 varchar,
	C_CITY varchar,
	C_STATE varchar,
	C_ZIP varchar,
	C_PHONE varchar,
	C_SINCE timestamp,
	C_CREDIT varchar,
	C_CREDIT_LIM decimal,
	C_DISCOUNT decimal,
	C_BALANCE decimal,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA varchar,
	PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
);

CREATE TABLE Orders (
	O_W_ID int,
	O_D_ID int,
	O_ID int,
	O_C_ID int,
	O_CARRIER_ID int,
	O_OL_CNT decimal,
	O_ALL_LOCAL decimal,
	O_ENTRY_D timestamp,
	PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
);

CREATE TABLE Items (
	I_ID int,
	I_NAME varchar,
	I_PRICE decimal,
	I_IM_ID int,
	I_DATA varchar,
	PRIMARY KEY (I_ID)
);

CREATE TABLE OrderLines (
	OL_W_ID int,
	OL_D_ID int,
	OL_O_ID int,
	OL_NUMBER int,
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT decimal,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY decimal,
	OL_DIST_INFO varchar,
	PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
);

CREATE TABLE Stocks (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY decimal,
	S_YTD decimal,
	S_ORDER_CNT int,
	S_REMOTE_CNT int,
	S_DIST_01 varchar,
	S_DIST_02 varchar,
	S_DIST_03 varchar,
	S_DIST_04 varchar,
	S_DIST_05 varchar,
	S_DIST_06 varchar,
	S_DIST_07 varchar,
	S_DIST_08 varchar,
	S_DIST_09 varchar,
	S_DIST_10 varchar,
	S_DATA text,
	PRIMARY KEY (S_W_ID, S_I_ID)
);

-- import data
copy warehouses (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) from 'warehouse.csv' with NULL = 'null';

copy districts (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) from 'district.csv' with NULL = 'null';

copy customers (c_w_id, c_d_id, c_id, c_first, c_middle, c_last,c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) from 'customer.csv' with NULL = 'null';

copy orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) from 'order.csv' with NULL = 'null';

copy items (i_id, i_name, i_price, i_im_id, i_data) from 'item.csv' with NULL = 'null';

copy orderlines (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) from 'order-line.csv' with NULL = 'null';

copy stocks (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) from 'stock.csv' with NULL = 'null';

EXIT;
