package com.pep.flink.mapper

import com.pep.flink.bean.{ UvSeparatedKeyModel}
import com.pep.flink.common.CommonProperties
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/*
class ProductProvinceUvRedisMapper(dataType:String,expireTime:Int)
  extends RedisMapper[UvSeparatedKeyModel] {

  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    var redisCommand: RedisCommand= null
    dataType match {
      case "product_province_uv" => redisCommand=RedisCommand.HSET
      case "product_province_index" => redisCommand=RedisCommand.LPUSH
    }
    new RedisCommandDescription(redisCommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: UvSeparatedKeyModel, context: SinkFunction.Context[_]): String = {
      getKeyByDiffrentType(data,context,dataType)
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
  override def getRedisKeyExpireTime(): Int = expireTime


  def getKeyByDiffrentType(data: UvSeparatedKeyModel, context: SinkFunction.Context[_],dataType:String) : String = {

    val processTime = context.currentProcessingTime
    val timeKey = DataUtils.queryTargetedBatchTimeStamp(processTime,"5s")
    var finalKey:String = null
    dataType match {
      case "product_province_uv" => {
        val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_province_uv_5s_prefix")
        finalKey = s"${data.productId}:${data.province}:${prefix}:${timeKey}"
      }
      case "product_province_index" =>{
        val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_province_uv_5s_index")
        finalKey = s"${data.productId}:${prefix}:${timeKey}"
      }
    }
    finalKey
  }
}
*/
