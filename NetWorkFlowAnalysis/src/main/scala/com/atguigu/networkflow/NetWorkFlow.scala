package com.atguigu.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 服务器后台日志
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// url流量统计
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
  * 实时流量统计
  */
object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDataStream: DataStream[String] = env.readTextFile("input/apache.log")
    inputDataStream.map(line => {
      val data = line.split(" ")
      val eventTime = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(data(3)).getTime
      ApacheLogEvent(data(0).trim, data(2).trim, eventTime, data(5).trim, data(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResult)
//        .print()
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))
      .print()

    env.execute("net work flow")
  }
}

class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(UrlViewCount(key, windowEnd, count))
  }
}

class TopNHotUrl(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 定义一个ListState保存数据
  private var listState: ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("hot url", classOf[UrlViewCount]))
  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 保存数据
    listState.add(value)
    // 启动定时器, 延时1ms
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 获取数据
    import  scala.collection.JavaConversions._
    val urlList: List[UrlViewCount] = listState.get().iterator().toList
    // 清除状态
    listState.clear()
    // 排序
    val topNUrlCount: List[UrlViewCount] = urlList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- topNUrlCount.indices) {
      val currentUrlView: UrlViewCount = topNUrlCount(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}