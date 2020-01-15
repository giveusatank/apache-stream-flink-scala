```
Flink处理实时大屏部分的数据获取规则
```

```
5s内各省用户使用分布：

（1）redis存储结构为set
（2）key为 ：${productId}:index:uv:5s:${timeStamp}

注：productId:为前端点击传入的产品Id
    timeStamp为秒级时间戳，且为5s的整数
```

```
各产品的实时用户数据：

（1）redis存储结构为set
（2）key为 ：${productId}:uv:1h:5s:${timeStamp}

注：productId:为前端点击传入的产品Id
    timeStamp为秒级时间戳，且为5s的整数
```

```
当天的实时用户UV：

（1）redis存储结构为set
（2）key为 ：${productId}:uv:today:5s:${timeStamp}

注：productId:为前端点击传入的产品Id
    timeStamp为秒级时间戳，且为5s的整数
```

```
当天的实时用户PV：

（1）redis存储结构为set
（2）key为 ：${productId}:pv:today:5s:${timeStamp}

注：productId:为前端点击传入的产品Id
    timeStamp为秒级时间戳，且为5s的整数
```