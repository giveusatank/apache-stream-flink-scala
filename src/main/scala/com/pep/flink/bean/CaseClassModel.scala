package com.pep.flink.bean

import java.util

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

case class ProductPvModel(productId:String,batchPv:String)

case class PvValueState(productId:String,currentPv:Int,timeStamp:Long)