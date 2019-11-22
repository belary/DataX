package com.alibaba.datax.plugin.writer.hivejdbcwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


public class HiveJdbcWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration writerSliceConfig = null;
        private HiveHelper hiveHelper;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            //检查
            hiveHelper = new HiveHelper();
            hiveHelper.validateParam(writerSliceConfig);
            LOG.debug("job init() 执行完成, originalConfig now is:\n{}\n",
                    writerSliceConfig.toJSON());
        }


        @Override
        public void preCheck(){
            this.init();
            //this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }


        /**
         * 这里执行job级别的所有presql
         */
        @Override
        public void prepare() {
            this.hiveHelper.prepare(this.writerSliceConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {

            //按照table的个数 切分
            LOG.info("分批导入HIVE表格开始");
            List<String> tables = this.writerSliceConfig.getList(Key.TABLE, String.class);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedConfig = null;

            // 一个查询语句返回一份配置，对应一个拆分，对应一组reader-writer线程足
            for (String table : tables) {
                splitedConfig = this.writerSliceConfig.clone();
                splitedConfig.set(Key.TABLE, table);
                readerSplitConfigs.add(splitedConfig);
            }
            return readerSplitConfigs;
        }

        @Override
        public void post() {

            //this.commonRdbmsWriterJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {

            //this.commonRdbmsWriterJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private static final String VALUE_HOLDER = "?";
        protected static String INSERT_TEMPLATE;

        protected Configuration writerSliceConfig;
        protected TaskPluginCollector taskPluginCollector;
        protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

        // hive导入的目标表
        protected String table;

        // 插入模式
        protected String writeMode;

        // hive 目标列
        protected List<String> columns;

        // 每批导入的数据条数和每批的内存大小限制
        protected int batchSize;
        protected int batchByteSize;

        // 目标列的数量
        protected int columnNumber = 0;

        // 最后执行的插入HIVE表的SQL
        protected String writeRecordSql;

        // 是否将NULL值作为空值插入
        protected boolean emptyAsNull;


        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.table = writerSliceConfig.getString(Key.TABLE);
            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();
            this.writeMode = writerSliceConfig.getString(Key.WRITE_MODE);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, com.alibaba.datax.plugin.rdbms.writer.Constant.DEFAULT_BATCH_SIZE);
            this.batchByteSize = writerSliceConfig.getInt(Key.BATCH_BYTE_SIZE, Constant.DEFAULT_BATCH_BYTE_SIZE);
            LOG.info("hive jdbc writer task init 初始化完成...");
        }

        @Override
        public void prepare() {
           // conn = jdbcDfsUtil.getConnection();
           //  LOG.info("hive jdbc 连接串已成功建立...");
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("开始执行HQL");
            //todo fc 这里的conn为空。不应该从jdbcDfsUtils中获取
            startWrite(recordReceiver, writerSliceConfig, super.getTaskPluginCollector());
            LOG.info("writer写数据完毕");

        }

        public void startWrite(RecordReceiver recordReceiver,
                               Configuration writerSliceConfig,
                               TaskPluginCollector taskPluginCollector) {
            String jdbcUrl = writerSliceConfig.getString(String.format("%s[0].%s",
                    com.alibaba.datax.plugin.writer.hivejdbcwriter.Constant.CONN_MARK, Key.JDBC_URL));
            String username = writerSliceConfig.getString(Key.USERNAME);
            String password = writerSliceConfig.getString(Key.PASSWORD);

            Connection connection = DBUtil.getConnection(DataBaseType.Hive,
                    jdbcUrl, username, password);

            startWriteWithConnection(recordReceiver, taskPluginCollector, connection);
        }

        public void startWriteWithConnection(RecordReceiver recordReceiver, TaskPluginCollector taskPluginCollector,
                                             Connection connection) {

            this.taskPluginCollector = taskPluginCollector;

            // 用于写入数据的时候的类型根据目的表字段类型转换
            this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                    this.table, StringUtils.join(this.columns, ","));

            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            int bufferBytes = 0;
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != this.columnNumber) {
                        // 源头读取字段列数与目的表字段写入列数不相等，直接报错
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                                record.getColumnNumber(),
                                                this.columnNumber));
                    }

                    writeBuffer.add(record);
                    bufferBytes += record.getMemorySize();

                    if (writeBuffer.size() >= batchSize || bufferBytes >= batchByteSize) {
                        doBatchInsert(connection, writeBuffer);
                        writeBuffer.clear();
                        bufferBytes = 0;
                    }
                }
                if (!writeBuffer.isEmpty()) {
                    doBatchInsert(connection, writeBuffer);
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                writeBuffer.clear();
                bufferBytes = 0;
                DBUtil.closeDBResources(null, null, connection);
            }
        }

        protected void doBatchInsert(Connection connection, List<Record> buffer)
                throws SQLException {
            PreparedStatement preparedStatement = null;
            try {

                for (Record record : buffer) {
                    preparedStatement = connection.prepareStatement(calcWriteRecordSql());
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.execute();
                }

            } catch (SQLException e) {
                LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneInsert(connection, buffer);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }


        protected void doOneInsert(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(
                                preparedStatement, record);
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        LOG.debug(e.toString());

                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    } finally {
                        // 最后不要忘了关闭 preparedStatement
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }

        // 直接使用了两个类变量：columnNumber,resultSetMetaData
        protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
                throws SQLException {
            for (int i = 0; i < this.columnNumber; i++) {
                int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
            }
            return preparedStatement;
        }

        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
            java.util.Date utilDate;
            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    preparedStatement.setString(columnIndex + 1, column
                            .asString());
                    break;

                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, longValue.toString());
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (this.resultSetMetaData.getRight().get(columnIndex)
                            .equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        } catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;

                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DataXException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                    break;

                case Types.BOOLEAN:
                case Types.BIT:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;

                default:
                    throw DataXException
                            .asDataXException(
                                    DBUtilErrorCode.UNSUPPORTED_TYPE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                            this.resultSetMetaData.getLeft()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getMiddle()
                                                    .get(columnIndex),
                                            this.resultSetMetaData.getRight()
                                                    .get(columnIndex)));
            }
            return preparedStatement;
        }

        /**
         * 计算批量插入的SQL语句
         * @return
         */
        private String calcWriteRecordSql() {

                List<String> valueHolders = new ArrayList<String>(columnNumber);
                for (int i = 0; i < columns.size(); i++) {
                    String type = resultSetMetaData.getRight().get(i);
                    valueHolders.add(VALUE_HOLDER);
                }

                INSERT_TEMPLATE = getWriteTemplate(columns, valueHolders, writeMode);
                return String.format(INSERT_TEMPLATE, this.table);
        }


        public  String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode) {

            boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert");

            if (!isWriteModeLegal) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持 insert on duplicate 方式. 请检查您的配置并作出修改.", writeMode));
            }

            // && writeMode.trim().toLowerCase().startsWith("replace")
            String writeDataSqlTemplate;
            writeDataSqlTemplate = new StringBuilder()
                    .append("INSERT INTO %s (").append(StringUtils.join(columnHolders, ","))
                    .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                    .append(")")
                    .append(onDuplicateKeyUpdateString(columnHolders))
                    .toString();

            return writeDataSqlTemplate;
        }


        public  String onDuplicateKeyUpdateString(List<String> columnHolders){
            if (columnHolders == null || columnHolders.size() < 1) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            sb.append(" ON DUPLICATE KEY UPDATE ");
            boolean first = true;
            for(String column:columnHolders){
                if(!first){
                    sb.append(",");
                }else{
                    first = false;
                }
                sb.append(column);
                sb.append("=VALUES(");
                sb.append(column);
                sb.append(")");
            }

            return sb.toString();
        }

        @Override
        public void post() {

            //this.commonRdbmsWriterTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {

            //this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
        }

        @Override
        public boolean supportFailOver(){
            return false;
        }

    }


}
