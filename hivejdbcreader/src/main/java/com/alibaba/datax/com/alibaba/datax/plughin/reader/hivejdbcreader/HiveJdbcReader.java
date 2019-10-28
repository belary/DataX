package com.alibaba.datax.com.alibaba.datax.plughin.reader.hivejdbcreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author fanchao on 2019/10/18.
 * @version v0.1
 * @desc hive jdbc reader
 */
public class HiveJdbcReader {

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

        private Configuration taskConfig;
        private String hiveSql;
        private String tmpPath;
        private String tableName;
        private DFSUtil dfsUtil = null;
        private HashSet<String> sourceFiles;

        @Override
        public void init() {
            //获取配置
            this.taskConfig = super.getPluginJobConf();//获取job 分割后的每一个任务单独的配置文件
            this.hiveSql = taskConfig.getString(Key.HIVE_SQL);//获取hive sql
            taskConfig.set(Key.FIELDDELIMITER, '\001');//设置hive 存储文件 hdfs默认的分隔符,传输时候会分隔
          //  tmpPath = Constant.TMP_PREFIX + KeyUtil.genUniqueKey();//创建临时Hive表 存储地址
            LOG.info("配置分隔符后:" + this.taskConfig.toJSON());
            //this.dfsUtil = new DFSUtil(this.taskConfig);//初始化工具类
        }


        @Override
        public void prepare() {
            //创建临时Hive表,指定存储地址


        }

        @Override
        public void startRead(RecordSender recordSender) {
            //读取临时hive表的hdfs文件

            LOG.info("end read source files...");
        }


        //只是局部post  属于每个task
        @Override
        public void post() {
            LOG.info("one task hive read post...");
            deleteTmpTable();
        }

        private void deleteTmpTable() {



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
