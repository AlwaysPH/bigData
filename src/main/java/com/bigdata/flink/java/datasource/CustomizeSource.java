package com.bigdata.flink.java.datasource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * flink连接自定义数据源
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class CustomizeSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.addSource(new MySource());

        streamSource.print();
        env.execute("CustomizeSource");

    }

    public static class MySource implements SourceFunction<String>{

        private Boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while (flag){
                for (int i = 0; i < 10; i++) {
                    ctx.collect("s" + i);
                }
                cancel();
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
