package com.bigdata.flink.java.tableApiAndSQL.udf.function;

import com.bigdata.flink.java.tableApiAndSQL.udf.Function;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * 自定义函数（UDF）-字段是否为空
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class FiledNotNullValidator extends ScalarFunction implements Function {

    private static final String VALIDATOR_NAME = "NOT_NULL";

    @Override
    public String getName() {
        return VALIDATOR_NAME;
    }

    public Boolean eval(String source) {
        if (source == null) {
            return false;
        }

        if (source instanceof String) {
            return !StringUtils.isNullOrWhitespaceOnly((String) source);
        }

        return true;
    }
}
