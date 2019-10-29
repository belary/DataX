package com.alibaba.datax;

import com.alibaba.datax.com.alibaba.datax.plughin.reader.hivejdbcreader.JDBCUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Unit test for simple App.
 */
public class AppTest 
{


    /**
     * 测试hive 查询
     */
    @Test
    public void checkSelectDataFromHive()
    {

        System.out.println("通过JDBC连接非Kerberos环境下的HiveServer2");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = JDBCUtils.getConnection();
            ps = connection.prepareStatement("select * from show_fact_web limit 10");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) );
            }
            Assert.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            JDBCUtils.disConnect(connection, rs, ps);
        }
    }


    /**
     * 测试hive 建表
     */
    @Test
    public void checkCreateTable()
    {

        Connection con = null;

        try {
            con = JDBCUtils.getConnection();
            Statement t2 = con.createStatement();
            t2.execute("CREATE TABLE fc_src(\n" +
                    "`content` string)\n" +
//                    "PARTITIONED BY (\n" +
//                    "  `pday` string)\n" +
                    "ROW FORMAT SERDE\n" +
                    "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                    "STORED AS INPUTFORMAT\n" +
                    "'org.apache.hadoop.mapred.TextInputFormat'\n" +
                    "OUTPUTFORMAT\n" +
                    "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

            Assert.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            JDBCUtils.disConnect(con, null, null);
        }
    }


    /**
     *  测试导入数据
     */
    @Test
    public void checkImportData()
    {

        Connection con = null;

        try {
            con = JDBCUtils.getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("SET hive.exec.dynamic.partition = true");
            stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
            stmt.executeQuery(
//            "set hive.exec.dynamic.partition=true;\n" +
//                    "set hive.exec.dynamic.partition.mode=nonstrict;\n" +
//                    "set hive.exec.max.dynamic.partitions.pernode=1000;\n" +
                    "from (select uuid \n " +
                    "from fc_hive_r20191012_15708715197724265121 limit 100) tmp \n" +
                    "insert overwrite table fc_show_fact_web PARTITION(pday='20191029') \n" +
                    "select uuid");

            Assert.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            JDBCUtils.disConnect(con, null, null);
        }
    }
}
