package com.pep.flink.sink

import com.pep.flink.bean.{ProductPvModel, PvValueState}
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProductTodayPvRedisMapper(expire: Int) extends RedisMapper[PvValueState] {
  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val rediscommand = RedisCommand.LPUSH
    new RedisCommandDescription(rediscommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: PvValueState, context: SinkFunction.Context[_]): String = {
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(data.timeStamp,"5s")
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_today_pv_day_5s_prefix")
    s"${data.productId}:${prefix}:${timeKey}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: PvValueState, context: SinkFunction.Context[_]): String = data.currentPv.toString

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire
}
