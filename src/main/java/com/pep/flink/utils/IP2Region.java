package com.pep.flink.utils;

import net.ipip.ipdb.City;
import net.ipip.ipdb.CityInfo;

import java.io.InputStream;

/**
 * 描述:
 * ip解析城市
 *
 * @author zhangfz
 * @create 2019-07-01 10:01
 */
public class IP2Region {
    private static City cityDb;
    private static InputStream inputStream;
    static{
        try {
            inputStream = IP2Region.class.getClassLoader().getResourceAsStream("ipipfree.ipdb");
            cityDb = new City(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String evaluate(String ip, int index) {
        String result = "";
        try {
            CityInfo info = cityDb.findInfo(ip.trim(), "CN");
            String country_name = info.getCountryName();
            String region_name = info.getRegionName();
            String city_name = info.getCityName();
            switch (index) {
                case 0 :
                    result = country_name;
                    break;
                case 1:
                    result = region_name;
                    break;
                case 2:
                    result = city_name;
                    break;
                default:
                    result = country_name;
            }
        } catch (Exception e) {
        }
        return result;
    }

    public static String evaluate(String ip) {
        try {
            CityInfo info = cityDb.findInfo(ip.trim(), "CN");
            String country_name = info.getCountryName();
            String region_name = info.getCityName();
            String city_name = info.getCityName();
            return country_name + "~" + region_name + "~" + city_name;
        } catch (Exception e) {
            return "";
        }
    }

    public static void main(String[] args) {
        System.out.println(IP2Region.evaluate("183.229.148.238", 1));

    }
}
