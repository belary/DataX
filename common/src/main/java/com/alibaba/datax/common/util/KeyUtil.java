package com.alibaba.datax.common.util;

import java.util.Random;

/**
 * @author dalizu on 2018/8/8.
 * @version v1.0
 * @desc 生成唯一的主键  时间+随机数
 */
public class KeyUtil {

    /**
     * 生成唯一的主键  时间+随机数
     *
     * synchronized  关键字
     *
     * */
    public static synchronized String genUniqueKey(){
        Random random=new Random();
        //六位随机数
        Integer num=random.nextInt(900000)+100000;

        return System.currentTimeMillis() +String.valueOf(num);


    }


}
