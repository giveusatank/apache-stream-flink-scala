package com.pep.flink.sink

import java.{lang, util}

import com.pep.flink.bean.ProductUvPiplineModel
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProductTodayUvRedisMapper(expire:Long) extends RedisMapper[ProductUvPiplineModel]{

  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val redisCommand = RedisCommand.PIPLINESADD
    new RedisCommandDescription(redisCommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: ProductUvPiplineModel, context: SinkFunction.Context[_]): String = {
    val timeKey = DataUtils.queryTodayTimeStamp
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_today_uv_day_5s_prefix")
    s"${data.productId}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: ProductUvPiplineModel, context: SinkFunction.Context[_]): String = null

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire.toInt

  /**
    * 添加Pipeline获取数据方法一
    */
  override def getPipelineArrayListFromData(data: ProductUvPiplineModel, context: SinkFunction.Context[_]): util.ArrayList[String] = data.userArray

  /**
    * 添加Pipeline获取数据方法二
    */
  override def getPipelineHashMapFromData(data: ProductUvPiplineModel, context: SinkFunction.Context[_]): util.HashMap[String, lang.Double] = null
}
