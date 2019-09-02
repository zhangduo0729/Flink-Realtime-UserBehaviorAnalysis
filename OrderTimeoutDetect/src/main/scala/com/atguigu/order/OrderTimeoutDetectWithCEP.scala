package com.atguigu.order

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 订单数据
case class OrderEvent(orderId: Long, orderType: String, orderTime: Long)
// 统计结果
case class OrderResult(orderId: Long, createTime: Long, result: String)

/**
  * 监控订单是否在15分钟内支付
  */
object OrderTimeoutDetectWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.fromCollection(Seq(
      OrderEvent(1001, "create", 1558430800),
      OrderEvent(1002, "create", 1558430805),
      OrderEvent(1003, "create", 1558430804),
      OrderEvent(1004, "create", 1558430820),
      OrderEvent(1003, "pay", 1558430880),
      OrderEvent(1001, "pay", 1558431400),
      OrderEvent(1005, "other", 1558431400),
      OrderEvent(1002, "pay", 1558431705)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.orderTime * 1000
      })
      .keyBy(_.orderId)

    // 定义CEP模式
    val orderPattern = Pattern.begin[OrderEvent]("create").where(_.orderType == "create")
        .followedBy("pay").where(_.orderType == "pay").within(Time.minutes(15))

    // 定义一个侧输出流用于接收超时的数据
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    // 将CEP应用于数据流
    val orderPatternStream = CEP.pattern(inputStream, orderPattern)

    // 取出数据
    val resultStream = orderPatternStream.select(orderTimeoutOutputTag, new OrderTimeoutMatch, new OrderSelectMatch)

    resultStream.print("orderSuccess")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("orderFail")

    env.execute("orderTimeoutDetect")
  }
}

// 超时数据结果
class OrderTimeoutMatch extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val createOrder = map.get("create").get(0)
    OrderResult(createOrder.orderId, createOrder.orderTime, "Unsuccessfully pay overtime 15minute")
  }
}

// 匹配到的数据结果
class OrderSelectMatch extends PatternSelectFunction[OrderEvent, OrderResult] {

  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val createOrder = map.get("create").get(0)
    val payOrder = map.get("pay").get(0)
    OrderResult(createOrder.orderId, createOrder.orderTime, s"Successfully pay in ${payOrder.orderTime - createOrder.orderTime}s")
  }
}
