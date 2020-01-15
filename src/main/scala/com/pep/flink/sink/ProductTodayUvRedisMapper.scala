package com.pep.flink.sink

import com.pep.flink.bean.ProductUvModel
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProductTodayUvRedisMapper(expire:Long) extends RedisMapper[ProductUvModel]{
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
  override def getKeyFromData(data: ProductUvModel, context: SinkFunction.Context[_]): String = {
    val processTime = context.currentProcessingTime()
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(processTime,"day")
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_today_uv_day_5s_prefix")
    s"${data.productId}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: ProductUvModel, context: SinkFunction.Context[_]): String = data.realUserId

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire.toInt
}
