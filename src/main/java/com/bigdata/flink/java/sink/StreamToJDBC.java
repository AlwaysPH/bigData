package com.bigdata.flink.java.sink;

import com.bigdata.flink.java.model.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * flink连接自定义数据源（jdbc）
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class StreamToJDBC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("data/ph.txt");

        DataStream<User> dataStream = source.map(e -> {
            String[] data = e.split(",");
            return new User(Integer.valueOf(data[0]), data[1], Integer.valueOf(data[2]), Long.valueOf(data[3]));
        });

        dataStream.addSink(new JdbcSink());
        env.execute();
    }

    //数据保存到自定义数据源中
    public static class JdbcSink extends RichSinkFunction<User>{

        Connection connection = null;
        PreparedStatement insert = null;
        PreparedStatement update = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("", "", "");
            insert = connection.prepareStatement("insert into table(id, name, age) values (?, ?, ?)");
            update = connection.prepareStatement("update table set name = ? where id = ?");
        }

        @Override
        public void invoke(User value, Context context) throws Exception {
            update.setString(1, value.getName());
            update.setInt(2, value.getId());
            update.execute();

            if(update.getUpdateCount() == 0){
                insert.setInt(1, value.getId());
                insert.setString(2, value.getName());
                insert.setInt(3, value.getAge());
                insert.execute();
            }
        }

        @Override
        public void close() throws Exception {
            update.close();
            insert.close();
            connection.close();
        }
    }
}
