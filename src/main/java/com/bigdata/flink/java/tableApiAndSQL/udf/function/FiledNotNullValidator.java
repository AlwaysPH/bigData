package com.bigdata.flink.java.tableApiAndSQL.udf.function;

import com.bigdata.flink.java.tableApiAndSQL.udf.Function;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * 自定义函数（UDF）-字段是否为空
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
@FunctionHint(
        input = {@DataTypeHint(inputGroup = InputGroup.ANY)},
        output = @DataTypeHint("Boolean")
)
public class FiledNotNullValidator extends ScalarFunction implements Function {

    private static final String VALIDATOR_NAME = "NOT_NULL";

    @Override
    public String getName() {
        return VALIDATOR_NAME;
    }

    public FiledNotNullValidator() {
    }

    //@DataTypeHint(inputGroup = InputGroup.ANY)接受任意类型输入
    public Boolean eval(Object source) {
        if (source == null) {
            return false;
        }

        if (source instanceof String) {
            return !StringUtils.isNullOrWhitespaceOnly((String) source);
        }

        return true;
    }
}
