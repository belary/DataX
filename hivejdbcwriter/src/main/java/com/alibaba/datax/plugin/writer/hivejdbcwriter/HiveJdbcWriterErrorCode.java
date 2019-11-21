package com.alibaba.datax.plugin.writer.hivejdbcwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HiveJdbcWriterErrorCode implements ErrorCode {

    BAD_CONFIG_VALUE("HiveReader-00", "您配置的值不合法."),
    SQL_NOT_FIND_ERROR("HiveReader-01", "您未配置hive sql"),
    DEFAULT_FS_NOT_FIND_ERROR("HiveReader-02", "您未配置defaultFS值"),
    ILLEGAL_VALUE("HiveReader-03", "值错误"),
    CONFIG_INVALID_EXCEPTION("HiveReader-04", "参数配置错误"),
    REQUIRED_VALUE("HiveReader-05", "您缺失了必须填写的参数值."),
    SCRIPT_ERROR("HiveReader-06", "hive 脚本执行失败."),
    UNSUPPORTED_TYPE("HiveReader-07", "不支持的数据库类型. 请注意查看 DataX 已经支持的数据库类型以及数据库版本."),;


    private final String code;
    private final String description;

    HiveJdbcWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}