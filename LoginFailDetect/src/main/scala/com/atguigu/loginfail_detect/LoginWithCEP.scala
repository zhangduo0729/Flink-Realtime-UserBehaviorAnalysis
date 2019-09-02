package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginWithCEP {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputDStream: DataStream[LoginEvent] = env.fromCollection(Seq(
//      LoginEvent(1, "192.168.1.11", "fail", 1558430842),
//      LoginEvent(1, "192.168.1.12", "fail", 1558430846),
//      LoginEvent(1, "192.168.1.13", "fail", 1558430844),
//      LoginEvent(1, "192.168.1.12", "fail", 1558430843),
//      LoginEvent(2, "192.168.1.18", "fail", 1558430845),
//      LoginEvent(2, "192.168.1.18", "success", 1558430845)
//    ))

    val inputDStream = env.socketTextStream("localhost", 44444)
      .map(line => {
        val data = line.split(",")
        LoginEvent(data(0).toLong, data(1), data(2), data(3).toLong)
      })

    val keyByStream: KeyedStream[LoginEvent, Long] = inputDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
    }).keyBy(_.userId)

    // 指定Pattern模式(连续两次时间类型都是登录失败, 且时间在两秒之内)
    val loginEventPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
        .next("next").where(_.eventType == "fail").within(Time.seconds(4))

    val loginFailPatternStream: PatternStream[LoginEvent] = CEP.pattern(keyByStream, loginEventPattern)
    // 获取匹配到的数据流
    loginFailPatternStream.select(new LoginFailMatch).print("fail")
    env.execute("login fail")
  }
}
// 自定义匹配函数, 取出匹配到的数据
class LoginFailMatch extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中可以按照模式的名称提取对应的登录失败事件
    val firstFail = map.get("begin").get(0)
    val lastFail = map.get("next").get(0)
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime)
  }
}