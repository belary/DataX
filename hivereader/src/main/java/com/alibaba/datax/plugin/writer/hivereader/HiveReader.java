package com.alibaba.datax.plugin.writer.hivereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.KeyUtil;
import com.alibaba.datax.common.util.ShellUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * @author dalizu on 2018/11/10.
 * @version v1.0
 * @desc hive reder
 */
public class HiveReader {

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

        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);


        private Configuration readerOriginConfig = null;


        @Override
        public void init() {
            LOG.info("init() begin...");

            //获取配置文件信息{parameter 里面的参数}
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();

            LOG.info("init() ok and end...");

        }


        private void validate() {

            this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HiveReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);

            List<String> sqls = this.readerOriginConfig.getList(Key.HIVE_SQL, String.class);
            if (null == sqls || sqls.size() == 0) {
                throw DataXException.asDataXException(
                        HiveReaderErrorCode.SQL_NOT_FIND_ERROR,
                        "您未配置hive sql");
            }

        }


        @Override
        public List<Configuration> split(int adviceNumber) {
            //按照Hive sql的个数 获取配置文件的个数
            LOG.info("split() begin...");
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

            tmpPath = Constant.TMP_PREFIX + KeyUtil.genUniqueKey();//创建临时Hive表 存储地址

            LOG.info("配置分隔符后:" + this.taskConfig.toJSON());
            this.dfsUtil = new DFSUtil(this.taskConfig);//初始化工具类

        }


        @Override
        public void prepare() {

            //创建临时Hive外部表
            // 1. 通过配置文件指定外部表的字段类型
            // 2. 通过S3上传获取外部表所关联的LOCATION地址

            tableName = hiveTableName();
            String hiveCmd = "create table " + tableName + " LOCATION '" + tmpPath + "' as " + hiveSql;

            LOG.info("hiveCmd ----> :" + hiveCmd);

            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveReaderErrorCode.SHELL_ERROR,
                        "创建hive临时表脚本执行失败");
            }

            LOG.info("创建hive 临时表结束 end!!!");

            LOG.info("prepare(), start to getAllFiles...");
            List<String> path = new ArrayList<String>();
            path.add(tmpPath);
            this.sourceFiles = dfsUtil.getAllFiles(path, Constant.TEXT);
            LOG.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]",
                    this.sourceFiles.size(),
                    StringUtils.join(this.sourceFiles, ",")));
        }

        @Override
        public void startRead(RecordSender recordSender) {
            //读取临时hive表的hdfs文件
            LOG.info("read start");
            for (String sourceFile : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", sourceFile));

                //默认读取的是TEXT文件格式
                InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig,
                        recordSender, this.getTaskPluginCollector());

                if (recordSender != null) {
                    recordSender.flush();
                }
            }
            LOG.info("end read source files...");
        }


        //只是局部post  属于每个task
        @Override
        public void post() {
            LOG.info("one task hive read post...");
            deleteTmpTable();
        }

        private void deleteTmpTable() {

            String hiveCmd = "drop table " + tableName;
            LOG.info("hiveCmd ----> :" + hiveCmd);
            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveReaderErrorCode.SHELL_ERROR,
                        "删除hive临时表脚本执行失败");
            }

        }

        @Override
        public void destroy() {
            LOG.info("hive read destroy...");
        }


        //创建hive临时表名称
        private String hiveTableName() {

            StringBuilder str = new StringBuilder();
            FastDateFormat fdf = FastDateFormat.getInstance("yyyyMMdd");

            str.append(Constant.TABLE_NAME_PREFIX).append(fdf.format(new Date()))
                    .append("_").append(KeyUtil.genUniqueKey());

            return str.toString();
        }

    }


}
