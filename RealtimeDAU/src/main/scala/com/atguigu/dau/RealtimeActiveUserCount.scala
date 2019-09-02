package com.atguigu.dau

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
  * 统计最近一小时的活跃用户数, 5分钟刷新统计
  */
object RealtimeActiveUserCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile("input/UserBehavior.csv")
      .map(line => {
        val dataArr = line.split(",")
        UserBehavior(dataArr(0).trim.toLong, dataArr(1).trim.toLong, dataArr(2).trim.toInt, dataArr(3).trim, dataArr(4).trim.toLong)
      })
      //env.socketTextStream("localhost", 44444)
      //  .map(line => {
      //    val dataArr = line.split(",")
      //    UserBehavior(dataArr(0).trim.toLong, dataArr(1).trim.toLong, dataArr(2).trim.toInt, dataArr(3).trim, dataArr(4).trim.toLong)
      //  })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .map((1, _))
      .keyBy(0)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .process(new CountProcessFunction)
      .print()

    env.execute()
  }
}

class CountProcessFunction extends ProcessWindowFunction[(Int, UserBehavior), String, Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, UserBehavior)], out: Collector[String]): Unit = {
    val startTime = new Timestamp(context.window.getStart)
    val endTime = new Timestamp(context.window.getEnd)
    //val count = elements.map(_._2.userId).toSet.size
    val count = elements.iterator.map { case (_, userBehavior) => (userBehavior.userId, 1) }.toMap.size
    Thread.sleep(500)
    out.collect(s"$startTime -> $endTime: $count")
  }
}