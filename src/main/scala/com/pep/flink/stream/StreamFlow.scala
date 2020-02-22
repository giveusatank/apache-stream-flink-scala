package com.pep.flink.stream


import java.util.HashMap
import java.util.concurrent.TimeUnit

import com.pep.flink.bean._
import com.pep.flink.common.ActionLogFormatter
import com.pep.flink.function._
import com.pep.flink.sink._
import com.pep.flink.utils.{DataUtils, KafkaPropertyUtils}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.api.common.time.{Time => CommonTime}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows

/**
  * 使用Flink作为流式计算引擎总结：
  *
  * （1）
  */

object StreamFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //开启检查点机制
    env.enableCheckpointing(120 * 1000)
    //设置并行度
    env.setParallelism(3)
    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
      100,
      CommonTime.of(5,TimeUnit.SECONDS)
    ) )
    //设置状态后端
    //env.setStateBackend(new FsStateBackend("hdfs://172.30.0.1:9000/flink/checkpoints"))

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

    val tempProvinceDs: WindowedStream[UvProxyKeyModel, String, TimeWindow] = batch5SDatastream
      .map(new UvMapFunction)
      .keyBy(_.proxyKey)
      .timeWindow(Time.seconds(5))
      /*.window(SlidingProcessingTimeWindows.of(Time.hours(1L),Time.seconds(5L)))*/

    //将数据索引存入Redis
    val productProvinceIndexDs: DataStream[ProvinceIndexModel] = tempProvinceDs
      .process[ProvinceIndexModel](new ProdProvinceUvIndexProcessFunction)


    //将数据存入Redis
    val productProvinceDs: DataStream[UvSeparatedKeyModel] = tempProvinceDs
      .process[UvSeparatedKeyModel](new ProdProvinceUvProcessFunction)


    val productKeyedDataStream: KeyedStream[ProductUvModel, String] = batch5SDatastream
      .map(new ProductUvMapFunction)
      .keyBy(_.productId)

    //滑动窗口计算最近一小时的各个产品的UV
    val productRecent1HourUvDs: DataStream[ProductUv] = productKeyedDataStream
      .timeWindow(Time.minutes(60),Time.seconds(5))
      .process[ProductUv](new ProductUvProcessFunction)
    /*val productRecent1HourUvDs: DataStream[ProductUvPipline] = productKeyedDataStream
      .timeWindow(Time.seconds(5))
      .process[ProductUvPipline](new Product1HourUvProcessFunction)*/

    //统计今日的各个产品的UV
    val productTodayUvDs: DataStream[ProductUvPiplineModel] = productKeyedDataStream
      .timeWindow(Time.seconds(5))
      .process[ProductUvPiplineModel](new ProductUvTodayProcessFunciton)

    //统计今日的各个产品的PV
    val productTodayPvDs: DataStream[PvValueState] = productKeyedDataStream
      .timeWindow(Time.seconds(5))
      .aggregate[Long,Long,PvValueState](new ProductPvAggregateFunction,new ProductPvWindowFunction)
      .keyBy(_.productId)
      .process[PvValueState](new ProductPvKeyedProcessFunciton)


    //创建Redis连接池
    val redisConf = DataUtils.getRedisConnect

    //数据大屏省份实时部分 增加sink
    val productProvinceIndexSink = new RedisSink[ProvinceIndexModel](redisConf, new ProdProvinceUvIndexRedisMapper(1*60*60))
    productProvinceIndexDs.addSink(productProvinceIndexSink).setParallelism(1)

    val productProvinceDataSink = new RedisSink[UvSeparatedKeyModel](redisConf, new ProdProvinceUvRedisMapper(1*60*60))
    productProvinceDs.addSink(productProvinceDataSink).setParallelism(1)

    //数据大屏产品UV实时部分 增加sink
    val productUvDataSink: RedisSink[ProductUv] = new RedisSink[ProductUv](redisConf,new ProductUvRedisMapper(1*60*60))
    productRecent1HourUvDs.addSink(productUvDataSink).setParallelism(1)

    //数据大屏各个产品今日UV统计 增加sink
    val productTodayUvDataSink = new RedisSink[ProductUvPiplineModel](redisConf,new ProductTodayUvRedisMapper(60*60*6))
    productTodayUvDs.addSink(productTodayUvDataSink).setParallelism(1)

    //数据大屏各个产品今日PV统计 增加sink
    val productTodayPvDataSink = new RedisSink[PvValueState](redisConf,new ProductTodayPvRedisMapper(60*60*6))
    productTodayPvDs.addSink(productTodayPvDataSink).setParallelism(1)


    //执行流式计算
    env.execute("yunwang-stream-job.")
  }

}
