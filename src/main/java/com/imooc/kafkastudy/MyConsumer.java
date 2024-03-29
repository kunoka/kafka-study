package com.imooc.kafkastudy;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class MyConsumer {
  private static KafkaConsumer<String, String> consumer;
  private static Properties properties;

  static {
    properties = new Properties();
    properties.put("bootstrap.servers", "127.0.0.1:9092");
    // 反序列化
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", "KafkaStudy");
  }

  private static void generalConsumeMessageAutoCommit() {
    properties.put("enable.auto.commit", true);
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton("imooc-kafka-study-x"));
    try {
      while (true) {
        boolean flag = true;
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
//          System.out.println(record);
          System.out.println(String.format(
            "topic = %s, partition = %s, key = %s, value = %s",
            record.topic(),
            record.partition(),
            record.key(),
            record.value()
          ));
          if (record.value().equals("done")) {
            flag = false;
          }
        }
        if (!flag) {
          break;
        }
      }
    } finally {
      consumer.close();
    }
  }

  // 同步提交
  private static void generalConsumeMessageSyncCommit() {
    properties.put("auto.commit.offset", false); // 手动设置位移
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton("imooc-kafka-study-x")); // topic name
    while (true) {
      boolean flag = true;
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(String.format(
          "topic = %s, partition = %s, key = %s, value = %s",
          record.topic(),
          record.partition(),
          record.key(),
          record.value()
        ));
        if (record.value().equals("done")) {
          flag = false;
        }
      }
      try {
        consumer.commitSync(); // 同步提交
      } catch (CommitFailedException ex) {
        System.out.println("commit failed error: " + ex.getMessage());
      }
      if (!flag) {
        break;
      }
    }
  }

  // 异步提交
  private static void generalConsumeMessageAsyncCommit() {
    properties.put("auto.commit.offset", false); // 手动设置位移
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton("imooc-kafka-study-x")); // topic name
    while (true) {
      boolean flag = true;
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(String.format(
          "topic = %s, partition = %s, key = %s, value = %s",
          record.topic(),
          record.partition(),
          record.key(),
          record.value()
        ));
        if (record.value().equals("done")) {
          flag = false;
        }
      }
      // commit A, offset 2000
      // commit B, offset 3000
      consumer.commitAsync(); // 异步提交（同步提交会重试，异步没有实现重试，重试会导致重复消费）
      if (!flag) {
        break;
      }
    }
  }

  private static void generalConsumeMessageAsyncCommitWithCallback() {
    properties.put("auto.commit.offset", false); // 手动设置位移
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton("imooc-kafka-study-x")); // topic name
    while (true) {
      boolean flag = true;
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(String.format(
          "topic = %s, partition = %s, key = %s, value = %s",
          record.topic(),
          record.partition(),
          record.key(),
          record.value()
        ));
        if (record.value().equals("done")) {
          flag = false;
        }
      }
      consumer.commitAsync((map, e) -> { // java8 新特性 函数式编程
        if (e != null) {
          System.out.println("commit failed for offsets: " + e.getMessage());
        }
      });
      if (!flag) {
        break;
      }
    }
  }

  // 混合提交
  @SuppressWarnings("all")
  private static void mixSyncAndAsyncCommit() {
    properties.put("auto.commit.offset", false); // 手动设置位移
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton("imooc-kafka-study-x")); // topic name
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(String.format(
            "topic = %s, partition = %s, key = %s, value = %s",
            record.topic(),
            record.partition(),
            record.key(),
            record.value()
          ));
        }
        consumer.commitAsync(); //异步提交 - 有可能抛出异常
      }
    } catch (Exception ex) {
      System.out.println("commit async error: " + ex.getMessage());
    } finally {
      // 异步提交失败，采用同步提交 保证消费消息位移能够提交
      try {
        consumer.commitSync();
      } finally {
        consumer.close();
      }
    }
  }

  public static void main(String[] args) {
    generalConsumeMessageAutoCommit();
  }
}
