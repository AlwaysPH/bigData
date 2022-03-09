package com.flink.java.state;

import com.flink.java.model.User;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink State Backends(状态后端)
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class StateBackend {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**设置状态后端**/
        //State 数据存储在TaskManager 内存中,Checkpoint 数据数据存储在jobManager 内存
        env.setStateBackend(new MemoryStateBackend());

        //State 数据存储在TaskManager 内存,Checkpoint:外部文件系统（本地或HDFS）
        env.setStateBackend(new FsStateBackend(""));

        //将所有的状态序列化之后, 存入本地的 RocksDB 数据库中.(一种 NoSql 数 据库, KV 形式存储)
        //State: TaskManager 中的KV数据库（实际使用内存+磁盘）
        //Checkpoint:外部文件系统（本地或HDFS）
        env.setStateBackend(new RocksDBStateBackend(""));

        DataStreamSource<String> text = env.socketTextStream("bigData04", 7777);

        SingleOutputStreamOperator<User> data = text.map(e -> {
            String[] str = e.split(",");
            return new User(Integer.valueOf(str[0]), str[1], Integer.valueOf(str[2]), Long.valueOf(str[3]));
        });

        env.execute();
    }
}
