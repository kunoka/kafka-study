package com.imooc.kafkastudy;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
  private static KafkaProducer<String, String> producer;

  static {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "127.0.0.1:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<String, String>(properties);
  }

  // 发送消息
  private static void sendMessageForgetResult() {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(
      "imooc-kafka-study", "name", "ForgetResult"
    );
    producer.send(record);
    producer.close();
  }

  public static void main(String[] args) {
    sendMessageForgetResult();
  }
}
