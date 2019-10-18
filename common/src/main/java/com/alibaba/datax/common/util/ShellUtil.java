package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author dalizu on 2018/11/10.
 * @version v1.0
 * @desc 执行shell脚本
 */
public class ShellUtil {

    private static final int SUCCESS = 0;

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);


    public static boolean exec(String [] command){

        try {
            Process process=Runtime.getRuntime().exec(command);

            read(process.getInputStream());
            StringBuilder errMsg= read(process.getErrorStream());
            // 等待程序执行结束并输出状态
            int exitCode = process.waitFor();
            if (exitCode == SUCCESS) {

                LOG.info("脚本执行成功");
                return true;
            } else {
                LOG.info("脚本执行失败[ERROR]:"+errMsg.toString());
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    private static StringBuilder  read(InputStream inputStream) {

        StringBuilder resultMsg=new StringBuilder();

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {

                resultMsg.append(line);
                resultMsg.append("\r\n");

            }

            return resultMsg;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


}
