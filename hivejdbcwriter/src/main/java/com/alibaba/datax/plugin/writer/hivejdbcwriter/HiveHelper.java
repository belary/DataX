package com.alibaba.datax.plugin.writer.hivejdbcwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;



public  class HiveHelper {

    public static final Logger LOG = LoggerFactory.getLogger(HiveJdbcWriter.Job.class);
    public static DataBaseType DATABASE_TYPE = DataBaseType.Hive;

    public void validateParam(Configuration originalConfig) {

        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, HiveJdbcWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, HiveJdbcWriterErrorCode.REQUIRED_VALUE);

        // 检查批量操作的设置
        doCheckBatchSize(originalConfig);

        // 检查连接串配置
        simplifyConf(originalConfig);

        // 检查列配置
        dealColumnConf(originalConfig);

        // 检查数据写模式设置
        dealWriteMode(originalConfig);
    }

    private void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(HiveJdbcWriterErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    private void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(HiveJdbcWriterErrorCode.REQUIRED_VALUE, "您未配置写入HIVE库的 jdbcUrl.");
            }

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(HiveJdbcWriterErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), tables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    private void dealColumnConf(Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String oneTable = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    private void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
            List<String> allColumns;
            if (isPreCheck){
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
            }else{
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
            }

            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (userConfiguredColumns.size() > allColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                DBUtil.getColumnMetaData(connectionFactory.getConnecttion(), oneTable,StringUtils.join(userConfiguredColumns, ","));
            }
        }
    }

    private  void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, DataBaseType.Hive, false);

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }


    // 执行preSql

    public void prepare(Configuration originalConfig) {
        int tableNumber = originalConfig.getInt(com.alibaba.datax.plugin.rdbms.writer.Constant.TABLE_NUMBER_MARK);
        if (tableNumber == 1) {
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);

            List<Object> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.writer.Constant.CONN_MARK,
                    Object.class);
            Configuration connConf = Configuration.from(conns.get(0)
                    .toString());

            // 这里的 jdbcUrl 已经 append 了合适后缀参数
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            originalConfig.set(Key.JDBC_URL, jdbcUrl);

            String table = connConf.getList(Key.TABLE, String.class).get(0);
            originalConfig.set(Key.TABLE, table);

            List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                    String.class);
            List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                    preSqls, table);

            originalConfig.remove(com.alibaba.datax.plugin.rdbms.writer.Constant.CONN_MARK);
            if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {

                // 说明有 preSql 配置，则此处删除掉
                originalConfig.remove(Key.PRE_SQL);

                Connection conn = DBUtil.getConnection(DataBaseType.Hive,
                        jdbcUrl, username, password);
                LOG.info("Begin to execute preSqls:[{}]. context info:{}.",
                        StringUtils.join(renderedPreSqls, ";"), jdbcUrl);

                WriterUtil.executeSqls(conn, renderedPreSqls, jdbcUrl, DataBaseType.Hive);
                DBUtil.closeDBResources(null, null, conn);
            }
        }

        LOG.debug("After job prepare(), originalConfig now is:[\n{}\n]",
                originalConfig.toJSON());
    }


}
