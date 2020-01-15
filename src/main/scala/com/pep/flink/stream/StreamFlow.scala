package com.pep.flink.stream


import java.util.HashMap

import com.pep.flink.bean._
import com.pep.flink.common.ActionLogFormatter
import com.pep.flink.function._
import com.pep.flink.sink._
import com.pep.flink.utils.{DataUtils, KafkaPropertyUtils}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink


object StreamFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(6)
    val kafkaProperty = KafkaPropertyUtils.getKafkaProp
    val rawDataStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("action_log_online", new SimpleStringSchema(), kafkaProperty))

    val dataModelDS: DataStream[DataModel] = rawDataStream.map(item => {
      val map: HashMap[String, String] = ActionLogFormatter.format(item)
      DataUtils.getDataModel(map)
    })

    //计算5S内各产品、省份的UV
    val batch5SDatastream = dataModelDS
      .filter(model => DataUtils.isNonEmpty(model.product_id) && DataUtils.isNonEmpty(model.province))

    //将数据存入Redis
    val productProvinceDs: DataStream[UvSeparatedKeyModel] = batch5SDatastream
      .map(new UvMapFunction)
      .keyBy(_.proxyKey)
      .timeWindow(Time.seconds(5))
      .process[UvSeparatedKeyModel](new ProdProvinceUvProcessFunction)
      .setParallelism(3)

    //将数据索引存入Redis
    val productProvinceIndexDs: DataStream[ProvinceIndexModel] = batch5SDatastream
      .map(new UvIndexMapFunction)
      .keyBy(_.productId)
      .timeWindow(Time.seconds(5))
      .process[ProvinceIndexModel](new ProdProvinceUvIndexProcessFunction)
      .setParallelism(3)


    val productKeyedDataStream: KeyedStream[ProductUvModel, String] = batch5SDatastream
      .map(new ProductUvMapFunction)
      .keyBy(_.productId)

    //滑动窗口计算最近一小时的各个产品的UV
    val productRecent1HourUvDs: DataStream[ProductUvModel] = productKeyedDataStream
      .timeWindow(Time.minutes(60),Time.seconds(5))
      .process[ProductUvModel](new ProductUvProcessFunction)
      .setParallelism(6)

    //统计今日的各个产品的UV
    val productTodayUvDs: DataStream[ProductUvModel] = productKeyedDataStream
      .timeWindow(Time.seconds(5))
      .process[ProductUvModel](new ProductUvProcessFunction)
      .setParallelism(3)

    //统计今日的各个产品的UV
    val productTodayPvDs: DataStream[PvValueState] = productKeyedDataStream
      .timeWindow(Time.seconds(5))
      .process[PvValueState](new ProductPvProcessFunction)
      .setParallelism(3)


    //创建Redis连接池
    val redisConf = DataUtils.getRedisConnect

    //数据大屏省份实时部分 增加sink
    val productProvinceIndexSink = new RedisSink[ProvinceIndexModel](redisConf, new ProdProvinceUvIndexRedisMapper(20 * 60 * 60))
    productProvinceIndexDs.addSink(productProvinceIndexSink).setParallelism(1)

    val productProvinceDataSink = new RedisSink[UvSeparatedKeyModel](redisConf, new ProdProvinceUvRedisMapper(20 * 60 * 60))
    productProvinceDs.addSink(productProvinceDataSink).setParallelism(1)

    //数据大屏产品UV实时部分 增加sink
    val productUvDataSink: RedisSink[ProductUvModel] = new RedisSink[ProductUvModel](redisConf,new ProductUvRedisMapper(20 * 60 * 60))
    productRecent1HourUvDs.addSink(productUvDataSink).setParallelism(1)

    //数据大屏各个产品今日UV统计 增加sink
    val productTodayUvDataSink = new RedisSink[ProductUvModel](redisConf,new ProductTodayUvRedisMapper(60 * 60 * 25))
    productTodayUvDs.addSink(productTodayUvDataSink).setParallelism(1)

    //数据大屏各个产品今日PV统计 增加sink
    val productTodayPvDataSink = new RedisSink[PvValueState](redisConf,new ProductTodayPvRedisMapper(60 * 60 * 25))
    productTodayPvDs.addSink(productTodayPvDataSink).setParallelism(1)


    //执行流式计算
    env.execute("yunwang-stream-job.")
  }

}
