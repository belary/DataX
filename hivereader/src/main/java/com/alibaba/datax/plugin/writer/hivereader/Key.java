package com.alibaba.datax.plugin.writer.hivereader;

/**
 * @author dalizu on 2018/11/10.
 * @version v1.0
 * @desc 常亮字段
 */
public class Key {

    //must have
    public final static String HIVE_SQL = "hiveSql";

    //must have
    public final static String DEFAULT_FS = "defaultFS";

    public final static String FIELDDELIMITER="fieldDelimiter";


    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

}
