-- CS4224 Project
-- Sep 19, 2015

-- Drop existing namespace
DROP KEYSPACE IF EXISTS cs4224;

-- Create Namespace
CREATE KEYSPACE cs4224 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TYPE cs4224.OrderLine (
	OL_I_ID int,
	OL_DELIVERY_D timestamp,
	OL_AMOUNT float,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY int,
	OL_DIST_INFO varchar
);

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
	W_TAX float,
	W_YTD float,
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
	D_TAX float,
	D_YTD float,
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
	C_CREDIT_LIM float,
	C_DISCOUNT float,
	C_BALANCE float,
	C_YTD_PAYMENT float,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
);

CREATE TABLE Customers_Unused (
  C_W_ID int,
	C_D_ID int,
	C_ID int,
  C_DATA varchar,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_ID)
);

CREATE TABLE Orders (
	O_W_ID int,
	O_D_ID int,
	O_ID int,
	O_C_ID int,
	O_CARRIER_ID int,
	O_OL_CNT int,
	O_ALL_LOCAL int,
	O_ENTRY_D timestamp,
	O_OLS map<int, frozen<OrderLine>>,
	PRIMARY KEY ((O_W_ID, O_D_ID), O_ID)
);

CREATE INDEX idx_o_c_id on Orders (o_c_id);
CREATE INDEX idx_o_carrier_id ON Orders (o_carrier_id);

CREATE TABLE Items (
	I_ID int,
	I_NAME varchar,
	I_PRICE float,
	PRIMARY KEY (I_ID)
);

CREATE TABLE Items_Unused (
  I_ID int,
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
	OL_AMOUNT float,
	OL_SUPPLY_W_ID int,
	OL_QUANTITY int,
	OL_DIST_INFO varchar,
	PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_NUMBER)
);

CREATE INDEX idx_ol_i_id ON OrderLines (ol_i_id);

CREATE TABLE Stocks (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY int,
	S_YTD float,
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
	PRIMARY KEY (S_W_ID, S_I_ID)
);

CREATE INDEX idx_s_quantity on Stocks (s_quantity);

CREATE TABLE Stocks_Unused (
  S_W_ID int,
	S_I_ID int,
	S_DATA varchar,
	PRIMARY KEY (S_W_ID, S_I_ID)
);

-- import data
copy warehouses (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) from 'warehouse.csv' with NULL = 'null';

copy districts (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) from 'district.csv' with NULL = 'null';


EXIT;
