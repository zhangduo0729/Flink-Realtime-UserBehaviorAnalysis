package com.atguigu.txmatch_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 来自两条流的交易匹配
  */
// 订单数据
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
// 交易信息
case class ReceiptEvent(txId: String, payTypeL: String, eventTime: Long)

object TxMatch {
  // 未匹配到pay信息的侧输出流标签
  private val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  private val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderInputStream = env.fromCollection(List(
      OrderEvent( 1, "create", "", 1558430842),
      OrderEvent( 2, "create", "", 1558430843),
      OrderEvent( 1, "pay", "111", 1558430844),
      OrderEvent( 2, "pay", "222", 1558430845),
      OrderEvent( 3, "create", "", 1558430849),
      OrderEvent( 3, "pay", "333", 1558430849)
    ))
      .filter(_.eventType == "pay")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(2)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000
      })
      .keyBy(_.txId)

    val receiptInputStream = env.fromCollection(List(
      ReceiptEvent( "111", "wechat", 1558430847),
      ReceiptEvent( "222", "alipay", 1558430848),
      ReceiptEvent( "444", "alipay", 1558430850)
    ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000
      })
      .keyBy(_.txId)

    val processStream = orderInputStream.connect(receiptInputStream).process(new connectProcessFunction)
    processStream.print("match")
    processStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute()
  }

  // 自定义CoProcessFunction处理两个流connect后的数据
  class connectProcessFunction extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    // 定义状态保存pay数据和receipt数据
    lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payEvent", classOf[OrderEvent]))
    lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptEvent", classOf[ReceiptEvent]))

    // 处理payEvent数据流
    override def processElement1(payEvent: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 获取receipt数据
      val receiptEvent: ReceiptEvent = receiptEventState.value()
      if (receiptEvent != null) {
        //两个事件都有, 正常匹配输出
        out.collect(payEvent, receiptEvent)
        receiptEventState.clear()
      } else {
        //没有匹配到receipt数据, 启动定时器等待receipt数据
        payEventState.update(payEvent)
        ctx.timerService().registerEventTimeTimer(payEvent.eventTime * 1000) //watermark已经设置了3s的延时, 所有这里就不设置等待的延迟时间
      }
    }

    // 处理receiptEvent数据流
    override def processElement2(receiptEvent: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 获取pay数据
      val payEvent: OrderEvent = payEventState.value()
      if (payEvent != null) {
        //两个事件都有, 正常匹配输出
        out.collect(payEvent, receiptEvent)
        payEventState.clear()
      } else {
        //没有匹配到pay数据, 启动定时器等待pay数据
        receiptEventState.update(receiptEvent)
        ctx.timerService().registerEventTimeTimer(receiptEvent.eventTime * 1000) //watermark已经设置了3s的延时, 所有这里就不设置等待的延迟时间
      }
    }

    // 定时器触发后的处理
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 获取payEvent和receiptEvent数据
      val payEvent: OrderEvent = payEventState.value()
      val receiptEvent: ReceiptEvent = receiptEventState.value()
      // 如果payEvent有数据, 说明没匹配到receiptEvent
      if (payEvent != null) {
        ctx.output(unmatchedPays, payEvent)
        payEventState.clear()
      }
      // 如果receiptEvent有数据, 说明没匹配到payEvent
      if (receiptEvent != null) {
        ctx.output(unmatchedReceipts, receiptEvent)
        receiptEventState.clear()
      }
    }
  }
}
