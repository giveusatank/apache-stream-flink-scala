package com.pep.flink.common;

import java.io.InputStream;
import java.util.Properties;

/**
 * 描述:
 *
 * @author zhangfz
 * @create 2019-10-31 16:50
 */
public class CommonProperties {
    public static Properties propScp;
    static {
        InputStream streamSCP = CommonProperties.class.getResourceAsStream("db.properties");
        propScp = new Properties();
        try {
            propScp.load(streamSCP);
            streamSCP.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
