package com.alibaba.datax.plugin.reader.hivejdbcreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
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

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            //按照Hive sql的个数 获取配置文件的个数
            LOG.info("分批查询HIVE开始");
            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            Configuration splitedConfig = null;
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

        private Configuration readerOriginConfig;
        private Connection conn;
        private JDBCUtils jdbcDfsUtil = null;

        @Override
        public void init() {
            //获取配置
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件
            this.validate();
            jdbcDfsUtil = new JDBCUtils(this.readerOriginConfig);
            conn = jdbcDfsUtil.getConnection();
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

            /**    0. 创建HIVE临时表，通过配置文件指定外部表的字段类型
              *    1. 通过HQL将查询结果导入到临时表中
             */



        }

        @Override
        public void startRead(RecordSender recordSender) {

            ResultSet rs = null;
            PreparedStatement ps = null;

            /**
             * 读取Hive外部表
             */
            LOG.info("begin to read source files...");
            try {

                //读取记录的元数据
                ps = conn.prepareStatement(readerOriginConfig.getString(Key.HIVE_SQL));
                rs = ps.executeQuery();
                int columnNumber = 0;
                ResultSetMetaData metaData = rs.getMetaData();
                columnNumber = metaData.getColumnCount();

                while (rs.next()) {
                    this.transportOneRecord(recordSender, rs,
                            metaData, columnNumber);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                jdbcDfsUtil.disConnect(conn, rs, ps);
            }


            LOG.info("end read source files...");

        }


        private Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber);
            recordSender.sendToWriter(record);
            return record;
        }
        private Record buildRecord(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber) {
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



                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
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
            LOG.info("one task hive read post...");
            deleteHiveExternalTableAndS3Files();
        }

        /**
         * 删除外部表格和相关的S3文件
         */
        private void deleteHiveExternalTableAndS3Files() {

        }

        @Override
        public void destroy() {
            LOG.info("hive read destroy...");
        }


        public static void main(String[] args) {
            try {
//                String prefix = "data/platback/logs/mysql/push/";
//                ObjectListing listObj = S3Util.listObj(prefix);
//
//                for (S3ObjectSummary objectSummary : listObj.getObjectSummaries()) {
//                    String key = objectSummary.getKey();
//                    System.out.println(key);
//                }


                S3Util.uploadObj( "data/platback/datax/tmp/0724-x1.txt", "/Users/fanchao/project/java/DataX/tmp/0724.txt");


            } catch (AmazonServiceException e) {
                // The call was transmitted successfully, but Amazon S3 couldn't process
                // it, so it returned an error response.
                e.printStackTrace();
            } catch (SdkClientException e) {
                // Amazon S3 couldn't be contacted for a response, or the client
                // couldn't parse the response from Amazon S3.
                e.printStackTrace();
            }
        }

    }


}
