package com.alibaba.datax.plugin.writer.hivewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.KeyUtil;
import com.alibaba.datax.common.util.ShellUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author dalizu on 2018/11/20.
 * @version v1.0
 * @desc
 */
public class HiveWriter extends Writer{


    public static class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        private String defaultFS;

        private String tmpPath;//hive 临时表指定的存储目录

        private String tmpTableName;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();//获取配置文件信息{parameter 里面的参数}
            log.info("hive writer params:{}", conf.toJSON());
            //校验 参数配置
            this.validateParameter();
        }

        private void validateParameter() {

            this.conf
                    .getNecessaryValue(
                            Key.DATABASE_NAME,
                            HiveWriterErrorCode.REQUIRED_VALUE);

            this.conf
                    .getNecessaryValue(
                            Key.TABLE_NAME,
                            HiveWriterErrorCode.REQUIRED_VALUE);


            this.conf
                    .getNecessaryValue(
                            Key.DEFAULT_FS,
                            HiveWriterErrorCode.REQUIRED_VALUE);

            this.conf
                    .getNecessaryValue(
                            Key.WRITE_MODE,
                            HiveWriterErrorCode.REQUIRED_VALUE);

        }

        @Override
        public void prepare() {
            String databaseName=this.conf.getString(Key.DATABASE_NAME);
            String tableName=this.conf.getString(Key.TABLE_NAME);
            log.info("job prepare start");
            String mode=this.conf.getString(Key.WRITE_MODE);//获取写入方式

            if(mode.equals(Constants.OVERWRITE_MODE)){
                //需要清空表，然后在执行所有task
                String prepareCmd ="use "+databaseName+";" +
                        "truncate table "+tableName+";";
                log.info("prepareCmd ----> :" + prepareCmd);

                //执行脚本,创建临时表
                if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + prepareCmd + "\""})) {
                    throw DataXException.asDataXException(
                            HiveWriterErrorCode.SHELL_ERROR,
                            "前置清空表失败");
                }

            }

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            this.defaultFS = this.conf.getString(Key.DEFAULT_FS);
            //按照reader 配置文件的格式  来 组织相同个数的writer配置文件
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);

            for (int i = 0; i < mandatoryNumber; i++) {

                Configuration splitedTaskConfig = this.conf.clone();

                tmpPath = Constants.TMP_PREFIX + KeyUtil.genUniqueKey();//创建临时Hive表 存储地址

                this.tmpTableName=hiveTableName();

                //后面需要指定写入的文件名称
                String fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                String fullFileName=String.format("%s%s/%s__%s", defaultFS, this.tmpPath,this.tmpTableName,fileSuffix);// 临时存储的文件路径

                splitedTaskConfig.set(Key.HIVE_TMP_PATH,tmpPath);
                splitedTaskConfig.set(Key.TMP_FULL_NAME,fullFileName);
                splitedTaskConfig.set(Key.TMP_TABLE_NAME,this.tmpTableName);
                //分区字段解析 "dt","type"
                List<String> partitions = this.conf.getList(Key.PARTITION, String.class);
                String partitionInfo=StringUtils.join(partitions,",");
                splitedTaskConfig.set(Key.PARTITION,partitionInfo);

                configurations.add(splitedTaskConfig);
            }
            return configurations;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        private String hiveTableName() {
            StringBuilder str = new StringBuilder();
            FastDateFormat fdf = FastDateFormat.getInstance("yyyyMMdd");

            str.append(Constants.TABLE_NAME_PREFIX).append(fdf.format(new Date()))
                    .append("_").append(KeyUtil.genUniqueKey());

            return str.toString();
        }

    }


    public static class Task extends Writer.Task {

        //写入hive步骤 (1)创建临时表  (2)读取数据写入临时表  (3) 从临时表写出数据

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration conf;

        private String databaseName;

        private String tmpTableName;

        private String tableName;//目标表名称

        private String writeMode;

        private boolean alreadyDel=false;

        private String tmpPath;

        private String defaultFS;

        private HdfsHelper hdfsHelper = null;//工具类


        @Override
        public void init() {

            this.conf = super.getPluginJobConf();

            //初始化每个task参数
            this.databaseName=this.conf.getString(Key.DATABASE_NAME);
            this.tableName=this.conf.getString(Key.TABLE_NAME);
            this.writeMode=this.conf.getString(Key.WRITE_MODE);
            this.defaultFS = this.conf.getString(Key.DEFAULT_FS);
            this.tmpTableName = this.conf.getString(Key.TMP_TABLE_NAME);

            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, conf);

        }


        @Override
        public void prepare() {
            //创建hive临时表,数据先写入临时表,空表

            this.tmpPath=this.conf.getString(Key.HIVE_TMP_PATH);


            //创建临时表，不分区
            List<Configuration>  columns = this.conf.getListConfiguration(Key.COLUMN);
            String columnsInfo=hdfsHelper.getColumnInfo(columns);

            String hiveCmd ="use "+this.databaseName+";" +
                            "create table " + this.tmpTableName + "("+columnsInfo+") " +
                            "stored as orc LOCATION '" + this.tmpPath+ "'";

            LOG.info("hiveCmd ----> :" + hiveCmd);

            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "创建hive临时表脚本执行失败");
            }

            addHook();
            LOG.info("创建hive 临时表结束 end!!!");

        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            //(2)读取数据写入临时表,默认创建的临时表是orc格式，效率比较高，占用磁盘空间小
            LOG.info("begin do write...");
            String fullFileName=this.conf.getString(Key.TMP_FULL_NAME);// 临时存储的文件路径
            LOG.info(String.format("write to file : [%s]", fullFileName));
            //写ORC FILE
            hdfsHelper.orcFileStartWrite(lineReceiver,this.conf, fullFileName,
                        this.getTaskPluginCollector());

            LOG.info("end do write tmp orc table");

            //获取分区字段
            String partitionInfo=this.conf.getString(Key.PARTITION);

            //从临时表写入到目标表

            String insertCmd="use "+this.databaseName+";" +
                             "SET hive.exec.dynamic.partition=true;" +
                             "SET hive.exec.dynamic.partition.mode=nonstrict;" +
                             "SET hive.exec.max.dynamic.partitions.pernode=1000;" +
                             "insert into table "+this.tableName+" partition("+partitionInfo+") " +
                             "select * from "+this.tmpTableName+";";

            LOG.info("insertCmd ----> :" + insertCmd);

            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + insertCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "导入数据到目标hive表失败");
            }

            LOG.info("end do write");
        }

        @Override
        public void post() {
            LOG.info("one task hive write post...end");
            deleteTmpTable();
        }

        @Override
        public void destroy() {

        }


        private void addHook(){
            if(!alreadyDel){
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        deleteTmpTable();
                    }
                }));
            }
        }


        private void deleteTmpTable() {

            String hiveCmd = "use "+this.databaseName+";" +
                             "drop table " + tmpTableName;//注意要删除的是临时表
            LOG.info("hiveCmd ----> :" + hiveCmd);
            //执行脚本,创建临时表
            if (!ShellUtil.exec(new String[]{"hive", "-e", "\"" + hiveCmd + "\""})) {
                throw DataXException.asDataXException(
                        HiveWriterErrorCode.SHELL_ERROR,
                        "删除hive临时表脚本执行失败");
            }
            alreadyDel=true;
        }

    }

}
