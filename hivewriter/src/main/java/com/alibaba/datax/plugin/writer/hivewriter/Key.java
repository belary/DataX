package com.alibaba.datax.plugin.writer.hivewriter;

/**
 * @author dalizu on 2018/11/20.
 * @version v1.0
 * @desc
 */
public class Key {

    //must have
    public final static String DATABASE_NAME = "databaseName";

    //must have
    public final static String TABLE_NAME = "tableName";

    //must have
    public final static String DEFAULT_FS = "defaultFS";

    // must have
    public static final String WRITE_MODE = "writeMode";

    public static final String HIVE_TMP_PATH="tmpPath";//临时hive表的location路径

    public static final String TMP_FULL_NAME="fullFileName";//临时hive 表  hdfs文件名称

    public static final String PARTITION="partition";

    public static final String TMP_TABLE_NAME="tmpTableName";


    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";

    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";

    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";


}
