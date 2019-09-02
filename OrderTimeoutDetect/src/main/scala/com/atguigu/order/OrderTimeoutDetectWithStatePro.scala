package com.atguigu.order

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 监控订单是否在15分钟内支付的优化方式
  */
object OrderTimeoutDetectWithStatePro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val processStream: DataStream[OrderResult] = env.socketTextStream("localhost", 4444)
      .map(line => {
        val dataArr = line.split(" ")
        OrderEvent(dataArr(0).trim.toLong, dataArr(1).trim, dataArr(2).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.orderTime * 1000
      })
      .keyBy(_.orderId)
      .process(new OrderPayMatch)

    processStream.print("order")
    processStream.getSideOutput(orderWarningOutputTag).print("warning")

    env.execute("orderTimeoutDetect")
  }

  // 定义侧输出流的标签, 用于获取侧输出流的订单异常数据
  private val orderWarningOutputTag = new OutputTag[OrderResult]("orderWarning")

  // 定义KeyedProcessFunction处理订单数据
  class OrderPayMatch extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    // 定义状态, 保存是否已经支付
    lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("pay state", classOf[Boolean]))
    // 用于保存定时器数据
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 获取之前的状态数据
      val isPay = isPayState.value()
      val time = timeState.value()

      // 判断当前数据的类型
      if (value.orderType == "create") {
        val ts = (value.orderTime + 900) * 1000  //create的时间15分钟后的时间
        // 判断是否已经支付
        if (isPay) {
          // 情况1: create事件, 且已pay
          if (time < ts) {
            // 情况1.1: pay在create后15分钟内
            out.collect(OrderResult(value.orderId, value.orderTime, "payed successfully"))
          } else {
            // 情况1.2: pay在create后15分钟外
            ctx.output(orderWarningOutputTag, OrderResult(value.orderId, value.orderTime, "payed but timeout"))
          }
          isPayState.clear()
          timeState.clear()
          ctx.timerService().deleteEventTimeTimer(time)
        } else {
          // 情况2: create事件, 但未pay
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }
      } else if (value.orderType == "pay") {
        if (time > 0) {
          // 情况3: pay事件, 且已create
          if (value.orderTime * 1000 < time) {
            // 情况3.1: pay在create后15分钟内
            out.collect(OrderResult(value.orderId, time / 1000 - 900, "payed successfully"))
          } else {
            // 情况3.2: pay在create后15分钟外
            ctx.output(orderWarningOutputTag, OrderResult(value.orderId, time, "payed but timeout"))
          }
          timeState.clear()
          ctx.timerService().deleteEventTimeTimer(time)
        } else {
          // 情况4: pay事件, 但未create(乱序事件)
          isPayState.update(true)
          ctx.timerService().registerEventTimeTimer(value.orderTime * 1000)
          timeState.update(value.orderTime * 1000)
        }
      }
    }

    // 定时器触发操作
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 情况2和情况4 两种情况会触发定时器
      if (isPayState.value()) {
        // 已经pay但没找到create事件
        ctx.output(orderWarningOutputTag, OrderResult(ctx.getCurrentKey, timeState.value() / 1000, "payed but not find created"))
      } else {
        // 已经create但超时未pay
        ctx.output(orderWarningOutputTag, OrderResult(ctx.getCurrentKey, timeState.value() / 1000 - 900, "order timeout"))
      }
      isPayState.clear()
      timeState.clear()
    }
  }

}