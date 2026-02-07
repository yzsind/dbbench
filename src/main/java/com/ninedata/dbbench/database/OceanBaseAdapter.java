package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class OceanBaseAdapter extends MySQLAdapter {
    public OceanBaseAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "OceanBase"; }
}
