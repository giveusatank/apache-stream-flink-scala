package com.pep.flink.bean


case class DataModel(deviceId:String,
                     active_user:String,
                     client_ip:String,
                     province:String,
                     product_id:String,
                     group:String,
                     action_title:String)

case class UvProxyKeyModel(proxyKey:String,realUserId:String)

case class UvSeparatedKeyModel(productId:String,province:String,userName:String)

case class ProvinceIndexModel(productId:String,province:String)

case class ProductUvModel(productId:String,realUserId:String)

case class ProductUvPiplineModel(productId:String,userArray:java.util.ArrayList[String])

case class ProductUv(productId:String,uv:Long)

case class ProductUvPipline(productId:String,userMap:java.util.HashMap[java.lang.String,java.lang.Double])

case class ProductPvModel(productId:String,batchPv:String)

case class PvValueState(productId:String,currentPv:Long)