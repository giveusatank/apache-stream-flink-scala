package com.pep.flink.function

import com.pep.flink.bean.{DataModel, ProductUvModel, ProvinceIndexModel, UvProxyKeyModel}
import com.pep.flink.utils.DataUtils
import org.apache.flink.api.common.functions.MapFunction


class UvMapFunction extends MapFunction[DataModel,UvProxyKeyModel]{
  override def map(model: DataModel): UvProxyKeyModel = {
    UvProxyKeyModel(s"${model.product_id}~${model.province}",
      DataUtils.getRealName(model.deviceId,model.active_user))
  }
}

class UvIndexMapFunction extends MapFunction[DataModel,ProvinceIndexModel]{
  override def map(model: DataModel): ProvinceIndexModel = {
    ProvinceIndexModel(model.product_id,model.province)
  }
}

class ProductUvMapFunction extends MapFunction[DataModel,ProductUvModel]{
  override def map(model: DataModel): ProductUvModel = {
    ProductUvModel(model.product_id,DataUtils.getRealName(model.deviceId,model.active_user))
  }
}
