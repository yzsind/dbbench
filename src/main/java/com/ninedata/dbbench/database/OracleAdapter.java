package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class OracleAdapter extends AbstractDatabaseAdapter {
    public OracleAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "Oracle"; }
    @Override protected String[] getCreateTableStatements() {
        return new String[]{
            "CREATE TABLE warehouse (w_id NUMBER NOT NULL, w_name VARCHAR2(10), w_street_1 VARCHAR2(20), w_street_2 VARCHAR2(20), w_city VARCHAR2(20), w_state CHAR(2), w_zip CHAR(9), w_tax NUMBER(4,4), w_ytd NUMBER(12,2), PRIMARY KEY (w_id))",
            "CREATE TABLE district (d_id NUMBER NOT NULL, d_w_id NUMBER NOT NULL, d_name VARCHAR2(10), d_street_1 VARCHAR2(20), d_street_2 VARCHAR2(20), d_city VARCHAR2(20), d_state CHAR(2), d_zip CHAR(9), d_tax NUMBER(4,4), d_ytd NUMBER(12,2), d_next_o_id NUMBER, PRIMARY KEY (d_w_id, d_id))",
            "CREATE TABLE customer (c_id NUMBER NOT NULL, c_d_id NUMBER NOT NULL, c_w_id NUMBER NOT NULL, c_first VARCHAR2(16), c_middle CHAR(2), c_last VARCHAR2(16), c_street_1 VARCHAR2(20), c_street_2 VARCHAR2(20), c_city VARCHAR2(20), c_state CHAR(2), c_zip CHAR(9), c_phone CHAR(16), c_since DATE, c_credit CHAR(2), c_credit_lim NUMBER(12,2), c_discount NUMBER(4,4), c_balance NUMBER(12,2), c_ytd_payment NUMBER(12,2), c_payment_cnt NUMBER, c_delivery_cnt NUMBER, c_data VARCHAR2(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "CREATE TABLE item (i_id NUMBER NOT NULL, i_im_id NUMBER, i_name VARCHAR2(24), i_price NUMBER(5,2), i_data VARCHAR2(50), PRIMARY KEY (i_id))",
            "CREATE TABLE stock (s_i_id NUMBER NOT NULL, s_w_id NUMBER NOT NULL, s_quantity NUMBER, s_dist_01 CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24), s_dist_04 CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24), s_dist_07 CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24), s_dist_10 CHAR(24), s_ytd NUMBER, s_order_cnt NUMBER, s_remote_cnt NUMBER, s_data VARCHAR2(50), PRIMARY KEY (s_w_id, s_i_id))",
            "CREATE TABLE history (h_c_id NUMBER, h_c_d_id NUMBER, h_c_w_id NUMBER, h_d_id NUMBER, h_w_id NUMBER, h_date DATE, h_amount NUMBER(6,2), h_data VARCHAR2(24))",
            "CREATE TABLE oorder (o_id NUMBER NOT NULL, o_d_id NUMBER NOT NULL, o_w_id NUMBER NOT NULL, o_c_id NUMBER, o_entry_d DATE, o_carrier_id NUMBER, o_ol_cnt NUMBER, o_all_local NUMBER, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "CREATE TABLE new_order (no_o_id NUMBER NOT NULL, no_d_id NUMBER NOT NULL, no_w_id NUMBER NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "CREATE TABLE order_line (ol_o_id NUMBER NOT NULL, ol_d_id NUMBER NOT NULL, ol_w_id NUMBER NOT NULL, ol_number NUMBER NOT NULL, ol_i_id NUMBER, ol_supply_w_id NUMBER, ol_delivery_d DATE, ol_quantity NUMBER, ol_amount NUMBER(6,2), ol_dist_info CHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }
}
