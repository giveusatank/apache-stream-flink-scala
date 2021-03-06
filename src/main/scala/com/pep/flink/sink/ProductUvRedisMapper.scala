package com.pep.flink.sink

import java.{lang, util}

import com.pep.flink.bean.ProductUv
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProductUvRedisMapper(expire:Int) extends RedisMapper[ProductUv]{
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
  override def getKeyFromData(data: ProductUv, context: SinkFunction.Context[_]): String = {
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(System.currentTimeMillis(),"5s")
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_recent_uv_1hour_5s_prefix")
    s"${data.productId}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: ProductUv, context: SinkFunction.Context[_]): String = data.uv.toString

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire

  /**
    * 添加Pipeline获取数据方法一
    */
  override def getPipelineArrayListFromData(data: ProductUv, context: SinkFunction.Context[_]): util.ArrayList[String] = null

  /**
    * 添加Pipeline获取数据方法二
    */
  override def getPipelineHashMapFromData(data: ProductUv, context: SinkFunction.Context[_]): util.HashMap[String, lang.Double] = null
}
