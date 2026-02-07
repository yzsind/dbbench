package com.ninedata.dbbench.database;

import com.ninedata.dbbench.config.DatabaseConfig;

public class DamengAdapter extends OracleAdapter {
    public DamengAdapter(DatabaseConfig config) { super(config); }
    @Override public String getDatabaseType() { return "Dameng"; }
}
