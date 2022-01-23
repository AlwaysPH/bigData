package com.bigdata.flink.java.tableApiAndSQL.udf.function;

import com.bigdata.flink.java.tableApiAndSQL.udf.Function;
import com.google.common.collect.Lists;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

import java.util.List;

/**
 * 自定义函数（UDF）-是否包含某个字段
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class ValueRangeValidator extends ScalarFunction implements Function {

    private static final String NAME = "VALUE_IN";

    private static final String DEFAULT_REGEX = ",";

    @Override
    public String getName() {
        return NAME;
    }

    public Boolean eval(String source, String rangeStr) {
        return eval(source, rangeStr, DEFAULT_REGEX);
    }

    public Boolean eval(String source, String rangeStr, String regex) {
        if (StringUtils.isNullOrWhitespaceOnly(rangeStr)) {
            return false;
        }

        if (source == null) {
            return false;
        }

        List<String> ranges = Lists.newArrayList(rangeStr.split(regex));
        return ranges.contains(source.toString());
    }
}
