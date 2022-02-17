package com.bigdata.flink.java.tableApiAndSQL.udf.function;

import com.bigdata.flink.java.tableApiAndSQL.udf.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义函数（UDF）-聚合函数
 * 计算年龄平均值
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class AvgAggFunction extends AggregateFunction<Integer, Tuple2<Integer, Integer>> implements Function {

    private static final String NAME = "AVG_AGG";

    public AvgAggFunction() {
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Integer getValue(Tuple2<Integer, Integer> accumulator) {
        return accumulator.f0 / accumulator.f1;
    }

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<Integer, Integer>(0, 0);
    }

    public void accumulate(Tuple2<Integer, Integer> acc, Integer age){
        acc.f0 += age;
        acc.f1 += 1;
    }
}
