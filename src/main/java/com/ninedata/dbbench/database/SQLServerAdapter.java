package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class SQLServerAdapter extends AbstractDatabaseAdapter {
    public SQLServerAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "SQL Server"; }
    @Override protected String[] getCreateTableStatements() {
        return new String[]{
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='warehouse') CREATE TABLE warehouse (w_id INT NOT NULL, w_name VARCHAR(10), w_street_1 VARCHAR(20), w_street_2 VARCHAR(20), w_city VARCHAR(20), w_state CHAR(2), w_zip CHAR(9), w_tax DECIMAL(4,4), w_ytd DECIMAL(12,2), PRIMARY KEY (w_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='district') CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_name VARCHAR(10), d_street_1 VARCHAR(20), d_street_2 VARCHAR(20), d_city VARCHAR(20), d_state CHAR(2), d_zip CHAR(9), d_tax DECIMAL(4,4), d_ytd DECIMAL(12,2), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customer') CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), c_street_1 VARCHAR(20), c_street_2 VARCHAR(20), c_city VARCHAR(20), c_state CHAR(2), c_zip CHAR(9), c_phone CHAR(16), c_since DATETIME, c_credit CHAR(2), c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2), c_ytd_payment DECIMAL(12,2), c_payment_cnt INT, c_delivery_cnt INT, c_data VARCHAR(500), PRIMARY KEY (c_w_id, c_d_id, c_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='item') CREATE TABLE item (i_id INT NOT NULL, i_im_id INT, i_name VARCHAR(24), i_price DECIMAL(5,2), i_data VARCHAR(50), PRIMARY KEY (i_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stock') CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, s_dist_01 CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24), s_dist_04 CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24), s_dist_07 CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24), s_dist_10 CHAR(24), s_ytd INT, s_order_cnt INT, s_remote_cnt INT, s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='history') CREATE TABLE history (h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT, h_date DATETIME, h_amount DECIMAL(6,2), h_data VARCHAR(24))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='oorder') CREATE TABLE oorder (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id, o_d_id, o_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='new_order') CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
            "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='order_line') CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT, ol_supply_w_id INT, ol_delivery_d DATETIME, ol_quantity INT, ol_amount DECIMAL(6,2), ol_dist_info CHAR(24), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))"
        };
    }
}
