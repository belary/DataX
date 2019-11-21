package com.alibaba.datax.plugin.writer.hivejdbcwriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.*;
import java.util.Set;

public class HiveJDBCUtils {

    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final Logger LOG = LoggerFactory.getLogger(HiveJDBCUtils.class);
    private Configuration hiveConf = Configuration.newDefault();

    HiveJDBCUtils(Configuration taskConfig) {

        // hive的配置参数
        Configuration hiveParam = taskConfig.getConfiguration(Key.CONN_URL);

        JSONObject hiveConnAsJsonObject = JSON.parseObject(taskConfig.getString(Key.CONN_URL));
        if (null != hiveParam) {
            Set<String> paramKeys = hiveConf.getKeys();
            for (String each : paramKeys) {
                hiveConf.set(each, hiveConnAsJsonObject.getString(each));
            }
        }

        //hive连接串
        hiveConf.set(Key.CONN_URL, taskConfig.getString(Key.CONN_URL));
        hiveConf.set(Key.USERNAME, taskConfig.getString(Key.USERNAME));
        hiveConf.set(Key.PASSWORD, taskConfig.getString(Key.PASSWORD));

        LOG.info(String.format("hiveConfig details:%s", JSON.toJSONString(this.hiveConf)));
    }

    /**
     * 加载jdbc驱动
     */
    static {
        try {
            Class.forName(DRIVER_NAME);
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取hive的jdbc连接
     * @return conn
     */
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(hiveConf.getString(Key.CONN_URL));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭数据库连接
     * @param connection
     * @param res
     * @param ps
     */
    public void closeConn(Connection connection, ResultSet res, PreparedStatement ps) {

        try {
            if (res != null)  res.close();
            if (ps != null) ps.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}