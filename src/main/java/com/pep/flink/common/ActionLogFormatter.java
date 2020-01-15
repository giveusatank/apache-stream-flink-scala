package com.pep.flink.common;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @Description:
 * @author:QZ
 * @date:2020/1/8 16:02
 */
public class ActionLogFormatter implements Serializable{
    private String id;//	varchar(40) comment 'ID'（由日志采集服务生成）
    private String remote_addr;//	String  远程访问地址（由日志采集服务生成）e.g.: [192.168.180.80]
    private String request_time;//	String  日志上传时间（由日志采集服务生成）e.g.: [1517214548.096]
    private String log_version;//	String   日志版本     e.g.:01
    private String start_time;//	bigint not null comment '时间：start_time开始时间',
    private String end_time;//	bigint comment '时间：end_time结束时间',
    private String region;//	varchar(32) not null comment '地点：region地区码或IP',
    private String product_id;//	varchar(32) not null comment '地点：产品或模块标识 依赖工程注册表',
    private String hardware;//	varchar(32) comment '地点：hardware依赖硬件 手机型号等',
    private String os;//	varchar(32) comment '地点：操作系统 android ios win8',
    private String soft;//	varchar(32) comment '地点：依赖软件soft,如ie8 .net4',
    private String active_user;//	varchar(32) comment '人物：active_user主动方人物标识、账号、id、证件、姓名等',
    private String active_organization;//	varchar(32) comment '人物：active_organization主动方机构ID、法人编号、部门名称等',
    private String active_type;// 	Int  10-人|20-程序及其他
    private String passive_object;//	varchar(32) comment '人物：passive_object主要被动方实体id',
    private String passive_type;//	varchar(32) not null comment '人物：passive_type被动方类型id', 开放允许第三方定义，在开放基础上固话一部分核心实体的枚举。
    private String from_product;//	varchar(32) not null comment '起因：from pos 从哪个模块/''产品来 依赖工程注册表',
    private String from_pos;//	varchar(32) comment '起因：from具体从哪个位置儿来,具体解释权在from_product指定的产品',
    private String company;//	varchar(32) comment '起因：所属合作公司'
    private String action_title;//	varchar(32) comment '经过：事件名',
    private String action_type;//	int not null comment '经过：type事件类型:[11-URL请求|21-业务动作|31-代码调用]',
    private String request;//	varchar(256) not null comment '经过：request请求地址、调用方法名',
    private String request_param;//	varchar(32) comment '经过：请求参数',
    private String group_type;//	int not null comment '经过：动作组类型，group type[1-单个动作|11-组内分动作|12-组开始动作或总动作|13-组结束动作]',
    private String group;//	varchar(32) not null comment '经过：group批次编号，如果没有就写sessionid、token',
    private String result_flag;//	int not null comment '结果：result flag结果标识[110-成功]',
    private String result;//	varchar(32) comment '结果：具体返回结果'

    //格式化日志内容
    public static HashMap<String,String> format(String content){
        String[] originalFragmentArray = (content.toString()+ "1").split("~");
        String[] fragmentArray = new String[27];
        int addSize = 27 - originalFragmentArray.length;
        for (int i = 0; i < addSize; i++) {
            fragmentArray[i] = "";
        }
        for (int i = 0; i < originalFragmentArray.length; i++) {
            fragmentArray[i + addSize] = originalFragmentArray[i];
        }
        Field[] fieldArray = ActionLogFormatter.class.getDeclaredFields();
        HashMap<String, String> contentMap = new LinkedHashMap();
        if (fragmentArray.length == fieldArray.length || fragmentArray.length + 1 == fieldArray.length) {
            for (int i = 0; i < fieldArray.length; i++) {
                //logger.error("date" + fieldArray[i].getName().toString() + ":" + fragmentArray[i]);
                contentMap.put(fieldArray[i].getName().toString(), fragmentArray[i]);

            }
        }
        return contentMap;
    }

    public static void main(String[] args) {

        String content = "28bd6508-5fdc-4a7b-b4e4-e65fd4952ba1~[112.249.251.20,112.249.251.20]~[1552985162.251]~2~1545125110552~~," +
                "~0cab0b45-2bdd-11e8-afa6-005056ae9a1b~m-type:ZUK k9,dpi:1080*1920~os:Android,version:6.0.1,c-type:,c-net-type:0," +
                "deviceId:867695024317199~~14~~~点击[取消标记]~~1.0.10.1~~~ry000007~~~~~1545124675439~~";

        long start = System.currentTimeMillis();
        for (int i = 0;i <1000000;i++){
            ActionLogFormatter.format(content).get("remote_addr");
        }

        System.out.println(System.currentTimeMillis()-start);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public String getLog_version() {
        return log_version;
    }

    public void setLog_version(String log_version) {
        this.log_version = log_version;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getEnd_time() {
        return end_time;
    }

    public void setEnd_time(String end_time) {
        this.end_time = end_time;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getProduct_id() {
        return product_id;
    }

    public void setProduct_id(String product_id) {
        this.product_id = product_id;
    }

    public String getHardware() {
        return hardware;
    }

    public void setHardware(String hardware) {
        this.hardware = hardware;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getSoft() {
        return soft;
    }

    public void setSoft(String soft) {
        this.soft = soft;
    }

    public String getActive_user() {
        return active_user;
    }

    public void setActive_user(String active_user) {
        this.active_user = active_user;
    }

    public String getActive_organization() {
        return active_organization;
    }

    public void setActive_organization(String active_organization) {
        this.active_organization = active_organization;
    }

    public String getActive_type() {
        return active_type;
    }

    public void setActive_type(String active_type) {
        this.active_type = active_type;
    }

    public String getPassive_object() {
        return passive_object;
    }

    public void setPassive_object(String passive_object) {
        this.passive_object = passive_object;
    }

    public String getPassive_type() {
        return passive_type;
    }

    public void setPassive_type(String passive_type) {
        this.passive_type = passive_type;
    }

    public String getFrom_product() {
        return from_product;
    }

    public void setFrom_product(String from_product) {
        this.from_product = from_product;
    }

    public String getFrom_pos() {
        return from_pos;
    }

    public void setFrom_pos(String from_pos) {
        this.from_pos = from_pos;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getAction_title() {
        return action_title;
    }

    public void setAction_title(String action_title) {
        this.action_title = action_title;
    }

    public String getAction_type() {
        return action_type;
    }

    public void setAction_type(String action_type) {
        this.action_type = action_type;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getRequest_param() {
        return request_param;
    }

    public void setRequest_param(String request_param) {
        this.request_param = request_param;
    }

    public String getGroup_type() {
        return group_type;
    }

    public void setGroup_type(String group_type) {
        this.group_type = group_type;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getResult_flag() {
        return result_flag;
    }

    public void setResult_flag(String result_flag) {
        this.result_flag = result_flag;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
