package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class TiDBAdapter extends MySQLAdapter {
    public TiDBAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "TiDB"; }
}
