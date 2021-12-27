package com.bigdata.hive;

import java.sql.*;

/**
 * hive jdbc测试
 */
public class HiveJDBCTest {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hive2://192.168.147.131:10000", "root", "");
        Statement statement = connection.createStatement();
//        String sql = "insert into test.test(id,name,age) values(3, 'heihei', 22)";
//        statement.execute(sql);
        String sql = "select *from test.test";
        ResultSet set = statement.executeQuery(sql);
        while (set.next()){
            System.out.println(set.getInt("id") + "~" + set.getString("name") + "~" + set.getInt("age"));
        }
    }
}
