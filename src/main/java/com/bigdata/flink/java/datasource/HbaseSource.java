package com.bigdata.flink.java.datasource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;


/**
 * flink读取HBASE数据
 * @author 彭红
 * @version 1.0
 * @date 2022-3-2 16:14
 */
public class HbaseSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> source = env.addSource(new HBaseReader());

        env.execute();
    }

    public static class HBaseReader extends RichSourceFunction<String> {

        private Connection connection = null;

        private Table table = null;

        private Scan scan = null;

        /**
         * 连接HBase客户端
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set(HConstants.ZOOKEEPER_QUORUM, "cgj-zhhw-mq01,cgj-zhhw-mq02,cgj-zhhw-mq03");
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
            conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 10000);
            conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,3000);
            TableName tableName = TableName.valueOf("test");
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            Iterator<Result> iterator = table.getScanner(scan).iterator();
            while(iterator.hasNext()) {
                Result next = iterator.next();
                String string = Bytes.toString(next.getRow());
                StringBuffer sb = new StringBuffer();
                for (Cell cell : next.listCells()) {
                    String s = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    sb.append("age " + s).append("_name ");
                }
                String result = sb.replace(sb.length() - 1, sb.length(), "").toString();
                Tuple2<String, String> tuple2 = new Tuple2<>(string, result);
                sourceContext.collect(tuple2.toString());
            }
        }

        @Override
        public void cancel() {

        }

        @Override
        public void close() throws Exception {
            if(null != connection){
                connection.close();
            }
            if(null != table){
                table.close();
            }
        }
    }
}
