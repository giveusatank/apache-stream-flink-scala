package com.pep.flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Description:
 * @author:QZ
 * @date:2020/1/8 16:05
 */
public class KafkaPropertyUtils {
    public static Properties prop = null;
    static {
        InputStream is = KafkaPropertyUtils.class.getClassLoader().getResourceAsStream("kafka.properties");
        Properties properties = new Properties();
        try {
            properties.load(is);
            prop = properties;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getKafkaProp(){
        return prop;
    }
}
