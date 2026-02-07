package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class DB2Adapter extends MySQLAdapter {
    public DB2Adapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "DB2"; }
}
