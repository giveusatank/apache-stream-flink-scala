package com.pep.flink.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Constants implements Serializable {
    public static final String geoIPFile = "GeoLite2-City.mmdb";
    public static final Map<String, String> provinceCN = new HashMap<String, String>();
    public static final Map<String, String> provinceMatch = new HashMap<String, String>();
    static {
        provinceCN.put("AH", "安徽");
        provinceCN.put("BJ", "北京");
        provinceCN.put("CQ", "重庆");
        provinceCN.put("FJ", "福建");
        provinceCN.put("GD", "广东");
        provinceCN.put("GS", "甘肃");
        provinceCN.put("GX", "广西");
        provinceCN.put("GZ", "贵州");
        provinceCN.put("HA", "河南");
        provinceCN.put("HB", "湖北");
        provinceCN.put("HE", "河北");
        provinceCN.put("HI", "海南");
        provinceCN.put("HK", "香港");
        provinceCN.put("HL", "黑龙江");
        provinceCN.put("HN", "湖南");
        provinceCN.put("JL", "吉林");
        provinceCN.put("JS", "江苏");
        provinceCN.put("JX", "江西");
        provinceCN.put("LN", "辽宁");
        provinceCN.put("MO", "澳门");
        provinceCN.put("NM", "内蒙古");
        provinceCN.put("NX", "宁夏");
        provinceCN.put("QH", "青海");
        provinceCN.put("SC", "四川");
        provinceCN.put("SD", "山东");
        provinceCN.put("SH", "上海");
        provinceCN.put("SN", "陕西");
        provinceCN.put("SX", "山西");
        provinceCN.put("TJ", "天津");
        provinceCN.put("TW", "台湾");
        provinceCN.put("XJ", "新疆");
        provinceCN.put("XZ", "西藏");
        provinceCN.put("YN", "云南");
        provinceCN.put("ZJ", "浙江");


        provinceMatch.put("安徽","AH");
        provinceMatch.put("北京","BJ");
        provinceMatch.put("重庆","CQ");
        provinceMatch.put("福建","FJ");
        provinceMatch.put("广东","GD");
        provinceMatch.put("甘肃","GS");
        provinceMatch.put("广西","GX");
        provinceMatch.put("贵州","GZ");
        provinceMatch.put("河南","HA");
        provinceMatch.put("湖北","HB");
        provinceMatch.put("河北","HE");
        provinceMatch.put("海南","HI");
        provinceMatch.put("香港","HK");
        provinceMatch.put("黑龙江","HL");
        provinceMatch.put("湖南","HN");
        provinceMatch.put("吉林","JL");
        provinceMatch.put("江苏","JS");
        provinceMatch.put("江西","JX");
        provinceMatch.put("辽宁","LN");
        provinceMatch.put("澳门","MO");
        provinceMatch.put("内蒙古","NM");
        provinceMatch.put("宁夏","NX");
        provinceMatch.put("青海","QH");
        provinceMatch.put("四川","SC");
        provinceMatch.put("山东","SD");
        provinceMatch.put("上海","SH");
        provinceMatch.put("陕西","SN");
        provinceMatch.put("山西","SX");
        provinceMatch.put("天津","TJ");
        provinceMatch.put("台湾","TW");
        provinceMatch.put("新疆","XJ");
        provinceMatch.put("西藏","XZ");
        provinceMatch.put("云南","YN");
        provinceMatch.put("浙江","ZJ");


    }
}
