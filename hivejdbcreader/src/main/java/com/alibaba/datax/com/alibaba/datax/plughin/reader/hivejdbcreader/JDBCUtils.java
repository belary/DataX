package com.alibaba.datax.com.alibaba.datax.plughin.reader.hivejdbcreader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class JDBCUtils {

    private static String CONNECTION_URL = "jdbc:hive2://10.100.6.108:10000/default";
    private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static String USER = "hive_rw";
    private static String PASSWORD = "s@g22d22bskjl";
    private static final Logger LOG = LoggerFactory.getLogger(JDBCUtils.class);

    /**
     * 加载jdbc驱动
     */
    static {
        try {
            Class.forName(DRIVER_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取hive的jdbc连接
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection() throws ClassNotFoundException,
            SQLException {
        return DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD);
    }

    /**
     * 关闭数据库连接
     * @param connection
     * @param res
     * @param ps
     * @throws SQLException
     */
    public static void disConnect(Connection connection, ResultSet res, PreparedStatement ps) {

        try {
            if (res != null)  res.close();
            if (ps != null) ps.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}