package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 登录日志
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
// 登录失败
case class Warning(userId: Long, firstFailEventTime: Long, lastFailEventTime: Long, warningMgs: String = "在2秒内连续登录失败2次")

/**
  * 2s内连续两次登录失败, 发出预警
  */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDStream: DataStream[LoginEvent] = env.fromCollection(Seq(
      LoginEvent(1, "192.168.1.11", "fail", 1558430842),
      LoginEvent(1, "192.168.1.12", "fail", 1558430843),
      LoginEvent(1, "192.168.1.12", "success", 1558430846),
      LoginEvent(1, "192.168.1.13", "fail", 1558430844),
      LoginEvent(2, "192.168.1.18", "fail", 1558430845),
      LoginEvent(2, "192.168.1.18", "success", 1558430845)
    ))

    inputDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
    })
      .keyBy(_.userId)
      .process(new LoginFailProcessFunction)
      .print("fail")

    env.execute("login fail")
  }
}
class LoginFailProcessFunction extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 定义一个ListState存储连续登录失败的数据
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login fail", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // TODO 方式1: 将连续失败登录的数据保存在状态中, 开启一个2s的定时器, 到时间后查看连续登录失败的次数是否大于2
//    if (value.eventType == "fail") {
//      loginFailListState.add(value)
//      ctx.timerService().registerEventTimeTimer((value.eventTime + 2) * 1000)
//    } else {
//      loginFailListState.clear()
//      ctx.timerService().deleteEventTimeTimer(value.eventTime * 1000)
//    }

    // TODO 方式2: 不用定时器, 比较连续的两次数据的是否都是失败, 且时间小于2s
    if (value.eventType == "fail") {
      val lastLoginFailIter = loginFailListState.get().iterator()
      if (lastLoginFailIter.hasNext) {
        val lastLoginFail: LoginEvent = lastLoginFailIter.next()
        if (value.eventTime <= lastLoginFail.eventTime + 2) {
          out.collect(Warning(value.userId, lastLoginFail.eventTime, value.eventTime))
          loginFailListState.clear()
        }
      }
      loginFailListState.add(value)
    } else loginFailListState.clear()
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    import scala.collection.JavaConversions._
    val loginFailList: List[LoginEvent] = loginFailListState.get().iterator().toList
    if (loginFailList.size >= 2) {
      out.collect(Warning(
        loginFailList(0).userId,
        loginFailList.head.eventTime,
        loginFailList.last.eventTime,
        s"在2秒内连续登录失败${loginFailList.size}次"
      ))
    } else {
      loginFailListState.clear()
    }
  }
}