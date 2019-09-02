package com.atguigu.hotitems.app


import java.io.InputStreamReader
import java.sql.Timestamp
import java.util.Properties

import com.atguigu.hotitems.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 热门实时商品统计
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间特性为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 文件源
    //    val fileInputStream: DataStream[String] = env.readTextFile("input/UserBehavior.csv")
    // 连接Kafka
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafkaConsumer.properties")))
    val kafkaInputStream = env.addSource(new FlinkKafkaConsumer[String]("hot_items", new SimpleStringSchema(), properties))

    kafkaInputStream.map(line => {
      val data = line.split(",")
      UserBehavior(data(0).trim.toLong, data(1).trim.toLong, data(2).trim.toInt, data(3).trim, data(4).trim.toLong * 1000)
    })
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
      override def extractTimestamp(element: UserBehavior): Long = element.timestamp
    })
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print()

    env.execute("hot items")
  }
}

/**
  * 自定义聚合函数, 统计每个商品的访问量
  */
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  // 累加器的初始值
  override def createAccumulator(): Long = 0L

  // 每一条数据加1 (类似于一个并行度内)
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  // 获取结果
  override def getResult(accumulator: Long): Long = accumulator

  // 聚合 (类似于多个并行度间)
  override def merge(a: Long, b: Long): Long = a + b
}

/**
  * 自定义窗口函数, 返回自定义的样例类
  */
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(key, windowEnd, count))
  }
}

/**
  * 自定义KeyedProcessFunction, 处理数据的排序
  *
  * @param n : TopN的值
  */
class TopNHotItems(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 自定义ListState, 用来保存所有的ItemViewCount
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item_stat", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 数据保存到ListState
    itemState.add(value)
    // 定义定时器, 延时100ms触发,;当定时器触发时, 等相同windowEnd的所有数据聚齐后, 统一排序处理, 相同的定时器只会触发一次
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发后, 意味着相同windowEnd的所有数据已经聚齐, 获取所有数据, 排序后排序
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    itemState.get().foreach(itemViewCount => allItems += itemViewCount)

    // 清理状态, 释放空间
    itemState.clear()

    // 排序
    //    allItems.sortWith(_.count > _.count).take(n)
    val topNList: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(n)

    // 拼接输出结果
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间: " + new Timestamp(timestamp - 100) + "\n")
    //for (i <- 0 until topNList.size) 下面是简化模式
    for (i <- topNList.indices) {
      // No1：  商品ID=12224  浏览量=2413
      result.append("No" + (i + 1) + ":")
      result.append(" 商品ID=" + topNList(i).itemId)
      result.append(" 浏览量=" + topNList(i).count + "\n")
    }
    result.append("====================================\n")

    // 输出
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}