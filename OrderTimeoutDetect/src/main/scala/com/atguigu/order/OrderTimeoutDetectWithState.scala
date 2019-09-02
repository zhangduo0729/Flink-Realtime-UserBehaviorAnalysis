package com.atguigu.order

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 监控订单是否在15分钟内支付
  */
object OrderTimeoutDetectWithState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.fromCollection(Seq(
      OrderEvent(1001, "create", 1558430800),
      OrderEvent(1002, "create", 1558430805),
      OrderEvent(1003, "create", 1558430804),
      OrderEvent(1004, "create", 1558430820),
      OrderEvent(1003, "pay", 1558430880),
      OrderEvent(1001, "pay", 1558431400),
      OrderEvent(1005, "other", 1558431400),
      OrderEvent(1002, "pay", 1558431706)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.orderTime * 1000
      })
      .keyBy(_.orderId)
      .process(new OrderProcessFunction)
      .print("order")

    env.execute("orderTimeoutDetect")
  }
}

class OrderProcessFunction extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  // 定义一个状态记录订单是否支付过
  lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is pay", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isPay = isPayState.value()

    if (value.orderType == "create" && !isPay)
      ctx.timerService().registerEventTimeTimer((value.orderTime + 900) * 1000)
    else if (value.orderType == "pay")
      isPayState.update(true)
  }

  // 15分钟后定时器触发, 判断是否支付过
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPay = isPayState.value()
    if (isPay) out.collect(OrderResult(ctx.getCurrentKey, timestamp / 1000 - 900, "order pay successfully"))
    else out.collect(OrderResult(ctx.getCurrentKey, timestamp / 1000 - 900, "order timeout"))
    isPayState.clear()
  }
}