package com.bigdata.flink.java.tablesql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class TableApiAndSQL {
    public static void main(String[] args) {
        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(setting);

        tEnv.executeSql("create table t (id int, name string)" +
                " with ('connector.type' = 'filesystem'," +
                "'connector.path' = 'data\\ph.txt'," +
                "'format.type' = 'csv')");
        Table result = tEnv.sqlQuery("select id, name from t where id < 3");
        result.execute().print();

    }
}
