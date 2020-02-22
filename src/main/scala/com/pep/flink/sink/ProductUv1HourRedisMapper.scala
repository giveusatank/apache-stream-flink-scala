package com.pep.flink.sink

import java.{lang, util}

import com.pep.flink.bean.{ProductUv, ProductUvPipline}
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class ProductUv1HourRedisMapper(expire:Int) extends RedisMapper[ProductUvPipline]{
  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val redisCommand = RedisCommand.ZSETSCORE
    new RedisCommandDescription(redisCommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: ProductUvPipline, context: SinkFunction.Context[_]): String = {
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
  override def getValueFromData(data: ProductUvPipline, context: SinkFunction.Context[_]): String = null

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire

  /**
    * 添加Pipeline获取数据方法一
    */
  override def getPipelineArrayListFromData(data: ProductUvPipline, context: SinkFunction.Context[_]): util.ArrayList[String] = null

  /**
    * 添加Pipeline获取数据方法二
    */
  override def getPipelineHashMapFromData(data: ProductUvPipline, context: SinkFunction.Context[_]):
  util.HashMap[java.lang.String, lang.Double] = data.userMap
}
