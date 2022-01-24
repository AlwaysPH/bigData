package com.bigdata.flink.java.tableApiAndSQL.udf.function;

import com.bigdata.flink.java.tableApiAndSQL.udf.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.StringUtils;

/**
 * 字符串切割-表函数
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class SplitDataTableFunction extends TableFunction<Tuple2<String, Integer>> implements Function {

    private static final String NAME = "SPLIT";

    private String splitFlag = ",";

    public SplitDataTableFunction(String splitFlag) {
        this.splitFlag = splitFlag;
    }

    public SplitDataTableFunction() {
    }

    @Override
    public String getName() {
        return NAME;
    }

    public void eval(String source){
        eval(source, splitFlag);
    }

    public void eval(String source, String flag){
        if (StringUtils.isNullOrWhitespaceOnly(source)) {
            return;
        }
        for(String i : source.split(flag)){
            this.collect(new Tuple2<String, Integer>(i, i.length()));
        }
    }
}
