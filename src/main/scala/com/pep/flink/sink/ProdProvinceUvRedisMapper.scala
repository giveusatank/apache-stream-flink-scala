package com.pep.flink.sink

import com.pep.flink.bean.UvSeparatedKeyModel
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class ProdProvinceUvRedisMapper(expire:Long) extends RedisMapper[UvSeparatedKeyModel]{
  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val rediscommand = RedisCommand.SADD
    new RedisCommandDescription(rediscommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: UvSeparatedKeyModel, context: SinkFunction.Context[_]): String = {
    val processTime = context.currentProcessingTime
    println(s"data1~~${processTime}")
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(processTime,"5s")
    println(s"data2~~${timeKey}")
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_province_uv_5s_prefix")
    s"${data.productId}:${data.province}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: UvSeparatedKeyModel, context: SinkFunction.Context[_]): String = data.userName

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire.toInt
}
