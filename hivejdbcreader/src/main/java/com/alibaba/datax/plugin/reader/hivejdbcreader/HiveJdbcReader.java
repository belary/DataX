package com.alibaba.datax.plugin.reader.hivejdbcreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author fanchao on 2019/10/18.
 * @version v0.1
 * @desc hive jdbc reader
 */
public class HiveJdbcReader extends Reader  {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerOriginConfig = null;

        @Override
        public void init() {

            LOG.info("插件初始化开始");

            //获取配置文件信息{parameter 里面的参数}
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();

            LOG.info("插件初始化成功完成");
        }

        /**
         * 验证配置文件格式
         */
        private void validate() {

            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);

            if (null == sqls || sqls.size() == 0) {
                throw DataXException.asDataXException(
                        HiveJdbcReaderErrorCode.SQL_NOT_FIND_ERROR,
                        "未配置查询HQL");
            }

        }


        @Override
        public void prepare() {
            LOG.info("hive jdbc reader job prepare 方法执行");

        }


        @Override
        public List<Configuration> split(int adviceNumber) {

            //按照Hive sql的个数 获取配置文件的个数
            LOG.info("分批查询HIVE开始");
            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedConfig = null;

            // 一个查询语句返回一份配置，对应一个拆分，对应一组reader-writer线程足
            for (String querySql : sqls) {
                splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Key.HIVE_SQL, querySql);
                readerSplitConfigs.add(splitedConfig);
            }
            return readerSplitConfigs;
        }

        //全局post
        @Override
        public void post() {
            LOG.info("任务执行完毕,hive reader post");
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        private Configuration readerOriginConfig;
        private Connection conn;
        private HiveJDBCUtils jdbcDfsUtil = null;

        @Override
        public void init() {
            //获取配置
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件
            this.validate();
            LOG.info("init() ok and end...");
        }

        void validate(){
            this.readerOriginConfig.getNecessaryValue(Key.CONN_URL, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
            this.readerOriginConfig.getNecessaryValue(Key.USER_NAME, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
            this.readerOriginConfig.getNecessaryValue(Key.PASSWORD, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
            this.readerOriginConfig.getNecessaryValue(Key.HIVE_SQL, HiveJdbcReaderErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {

            jdbcDfsUtil = new HiveJDBCUtils(this.readerOriginConfig);
            conn = jdbcDfsUtil.getConnection();
            LOG.info("hive jdbc 连接串已成功建立...");
        }

        @Override
        public void startRead(RecordSender recordSender) {

            ResultSet rs = null;
            PreparedStatement ps = null;

            LOG.info("开始执行HQL");
            try {

                //执行查询，并解析记录的元数据
                ps = conn.prepareStatement(readerOriginConfig.getString(Key.HIVE_SQL));
                rs = ps.executeQuery();
                int columnNumber = 0;
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                //逐条数据发送到writer线程
                while (rs.next()) {
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber, super.getTaskPluginCollector());
                }
                LOG.info("HQL执行成功");
            } catch (SQLException e) {
                LOG.error("HQL执行异常");
                LOG.error("HQL读取数据发生异常:", e);
            } finally {
                jdbcDfsUtil.closeConn(conn, rs, ps);
            }
            LOG.info("reader数据发送完毕");
        }


        /**
         * 将读取的记录发送到writer
         * @param recordSender
         * @param rs
         * @param metaData
         * @param columnNumber
         * @param taskPluginCollector
         * @return
         */
        private Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber,
                                          TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber, taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        /**
         * 建立需要发送的记录实例
         * @param recordSender
         * @param rs
         * @param metaData
         * @param columnNumber
         * @param taskPluginCollector
         * @return
         */
        private Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData,
                                   int columnNumber, TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.VARCHAR:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;


                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                        case Types.FLOAT:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if(rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            HiveJdbcReaderErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }

            }
            return record;
        }

        @Override
        public void post() {
            /**
             *  读取完成以后这里可以放置一些校验工作
             */
            LOG.info("hive jdbc reader task post方法执行");
        }


        @Override
        public void destroy() {
            LOG.info("hive jdbc reader task destroy方法执行");
        }

    }


}
