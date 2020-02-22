package com.pep.flink.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, HashMap}

import com.pep.flink.bean.DataModel
import com.pep.flink.common.Constants
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig


object DataUtils {

  def getDataModel(map: HashMap[String, String]): DataModel = {
    val log_version: String = map.getOrDefault("log_version", "")
    val os: String = map.getOrDefault("os", "")
    val group: String = map.getOrDefault("group", "")
    val hardware: String = map.getOrDefault("hardware", "")
    val active_user: String = map.getOrDefault("active_user", "")
    val remote_addr: String = map.getOrDefault("remote_addr", "")
    var product_id: String = map.getOrDefault("product_id", "")
    var company: String = map.getOrDefault("company", "")
    if (product_id.size > 2 && product_id != "1213") product_id = product_id.substring(0, 3)
    if (company == "pep_click") product_id = "1214"
    val action_title: String = map.getOrDefault("action_title", "")
    val deviceId = getDeviceId(log_version, os, hardware, group, active_user, action_title)
    DataModel(deviceId, active_user, getRealIp(remote_addr), getIpLocation(remote_addr), product_id, group, action_title)
  }

  def getDeviceId(log_version: String, os: String, hardware: String, group: String, active_user: String, action_title: String): String = {
    var deviceId: String = null
    if (os.indexOf("Windows") > -1) {
      if (hardware.indexOf("deviceId") > -1) {
        //dpi:1366*768,factor:1,mem:6042MB,deviceId:c4a880505c2c4922b4d3008c2aeedd2f
        val startLen = hardware.indexOf("deviceId:") + "deviceId:".length
        val lastPointIndex = hardware.substring(hardware.indexOf("deviceId:") + "deviceId:".length).indexOf(",")
        if (lastPointIndex < 0) {
          deviceId = hardware.substring(hardware.indexOf("deviceId:") + "deviceId:".length)
        } else {
          deviceId = hardware.substring(hardware.indexOf("deviceId:") + "deviceId:".length, lastPointIndex + startLen)
        }
      } else {
        //1920X1080,1.25,3489MB,x86,x86 Family 6 Model 58 Stepping 9, GenuineIntel,ec7fc1774bed4a678e19a4ed875a4af9
        if (hardware.split(",").size > 6) {
          deviceId = hardware.split(",")(6)
        }
      }
    } else {
      if (log_version != 1 && os.indexOf("deviceId:") > -1) {
        //(os:Android,version:6.0.1,c-type:中国移动,c-net-type:0,deviceId:861084034342768,30)
        val startLen = os.indexOf("deviceId:") + "deviceId:".length
        val lastPointIndex = os.substring(os.indexOf("deviceId:") + "deviceId:".length).indexOf(",")
        if (lastPointIndex < 0) {
          deviceId = os.substring(os.indexOf("deviceId:") + "deviceId:".length)
        } else {
          deviceId = os.substring(os.indexOf("deviceId:") + "deviceId:".length, lastPointIndex + startLen)
        }
      } else {
        if (os.indexOf("nDeviceId(IMEI):") > -1) {
          //(nDeviceId(IMEI):863247035536534,nDeviceSoftwareVersion:00,nLine1Number:,nNetworkCountryIso:cn,nNetworkOperator:46001,nNetworkOperatorName:CHN-UNICOM,nNetworkType:13,nPhoneType:1,nSimCountryIso:cn,nSimOperator:46001,nSimOperatorName:,nSimSerialNumber:89860115881008025450,nSimState:5,nSubscriberId(IMSI):460014489017834,nVoiceMailNumber:null,1)
          deviceId = os.substring(os.indexOf("nDeviceId(IMEI):") + "nDeviceId(IMEI):".length, os.indexOf(","))
        }
      }
    }
    //网站类使用userID作为用户标识
    if (action_title.equals("sys_200001")) {
      deviceId = active_user
    }
    if ((deviceId == null || deviceId == "null" || deviceId.length < 1) && !action_title.equals("sys_200001")) {
      deviceId = group
    }
    deviceId
  }

  def getRealIp(remote_addr: String): String = {
    var real_ip = ""
    if (remote_addr != null && !remote_addr.isEmpty) {
      real_ip = remote_addr.replace("[", "").replace("]", "").split(",")(0)
    }
    real_ip
  }

  def getIpLocation(remote_addr: String): String = {
    var province: String = ""
    if (remote_addr != null && !remote_addr.isEmpty) {
      province = Constants.provinceMatch.get(IP2Region.evaluate(getRealIp(remote_addr), 1))
    }
    province
  }

  def isNonEmpty(ele: String): Boolean = {
    ele match {
      case null => false
      case "null" => false
      case "" => false
      case _ => true
    }
  }

  def getRealName(deviceId:String,userId:String):String = {
    if(isNonEmpty(deviceId)) deviceId else userId
  }

  //获取指定时间批次的时间戳整形
  def queryTargetedBatchTimeStamp(current: Long, batchType: String): Long = {
    val cur = current / 1000
    val mod: Int = batchType match {
      case "5s" => 5
      case "10s" => 10
      case "minute" => 60
      case "hour" => 3600
    }
    cur - (cur % mod)
  }

  //获取今天的整点时间戳
  def queryTodayTimeStamp(): String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    (format.parse(format.format(new Date())).getTime / 1000 ).toString
  }

  // 获取当前一小时之前的秒级时间戳
  def getLastHourSecondTS() : Long = {
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.HOUR,-1)
   calendar.getTime.getTime / 1000
  }

  def getRedisConnect(): FlinkJedisPoolConfig = {
    val prop = RedisPropertyUtils.getRedisProperty
    val maxIdle = prop.getProperty("maxIdle").toInt
    val maxTotal = prop.getProperty("maxTotal").toInt
    val minIdle = prop.getProperty("minIdle").toInt
    val host = prop.getProperty("host")
    val port = prop.getProperty("port").toInt
    val password = prop.getProperty("password")
    val databaseId = prop.getProperty("database").toInt
    val timeOut = prop.getProperty("timeout").toInt
    new FlinkJedisPoolConfig.Builder().setHost(host).setMaxIdle(maxIdle).setMaxTotal(maxTotal)
      .setMinIdle(minIdle).setPort(port).setPassword(password).setDatabase(databaseId).setTimeout(timeOut).build()
  }

  def main(args: Array[String]): Unit = {
    //println(judgeTimeStampIsOneDay(1578984539000L, 1578986669000L))
    println(queryTodayTimeStamp)

    println(isNonEmpty("null"))
    println(isNonEmpty(""))
    println(isNonEmpty(null))
    println(isNonEmpty("123"))

    println(queryTargetedBatchTimeStamp(1581782647461L,"5s"))
    println(queryTargetedBatchTimeStamp(1581782647461L,"minute"))
    println(queryTargetedBatchTimeStamp(1581782647461L,"hour"))

    println(getLastHourSecondTS)
  }
}
