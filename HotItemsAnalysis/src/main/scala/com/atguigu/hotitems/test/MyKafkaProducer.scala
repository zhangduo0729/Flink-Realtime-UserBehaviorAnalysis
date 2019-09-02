package com.atguigu.hotitems.test

import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.{BufferedSource, Source}

object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    witeToKafka("hot_items")
  }
  def witeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("kafkaProducer.properties")))

    val producer = new KafkaProducer[String, String](properties)
    val source: BufferedSource = Source.fromFile("input/UserBehavior.csv")
    for (line <- source.getLines()) {
      producer.send(new ProducerRecord[String, String](topic, line))
    }

    producer.close()
  }
}
