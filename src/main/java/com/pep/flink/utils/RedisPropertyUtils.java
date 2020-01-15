package com.pep.flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Description:
 * @author:QZ
 * @date:2020/1/13 11:01
 */
public class RedisPropertyUtils {

    static Properties properties = null;
    static {
        properties = new Properties();
        InputStream is = RedisPropertyUtils.class.getClassLoader().getResourceAsStream("redis.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getRedisProperty(){
        return properties;
    }
}
