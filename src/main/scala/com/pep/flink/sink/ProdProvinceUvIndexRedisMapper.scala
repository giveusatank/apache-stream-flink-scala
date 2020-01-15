package com.pep.flink.sink

import com.pep.flink.bean.ProvinceIndexModel
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProdProvinceUvIndexRedisMapper(expire: Long) extends RedisMapper[ProvinceIndexModel] {
  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val rediscommand = RedisCommand.SADD
    new RedisCommandDescription(rediscommand)
  }

  override def getKeyFromData(data: ProvinceIndexModel, context: SinkFunction.Context[_]): String = {
    val processTime = context.currentProcessingTime
    println(s"index1~~${processTime}")
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(processTime,"5s")
    println(s"index2~~${timeKey}")
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_province_uv_5s_index")
    s"${data.productId}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: ProvinceIndexModel, context: SinkFunction.Context[_]): String = data.province


  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire.toInt
}
