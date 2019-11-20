package com.alibaba.datax.plugin.reader.hivejdbcreader;

/**
 * @author fanchao on 2019/11/19
 * @version v1.0
 * @desc 常量字段与配置文件字段名称同步，防止代码中出现字符串硬编码
 */
public class Key {

    //hive集群的连接串
    final static String CONN_URL="conn_url";
    final static String USER_NAME="user_name";
    final static String PASSWORD="password";

    //reader的查询hql
    final static String HIVE_SQL = "hive_sql";

    //hive 的配置参数
    final static String HIVE_CONF = "hive_conf";






}
