package com.cloudera.sa.secureconsumer;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author vsingh
 */
public class SecureAsyncKafkaConsumer {

  private final Properties props = new Properties();

  public void setUp() {
    props.put("bootstrap.servers", "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092,"
        + "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092");
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    System.setProperty("java.security.auth.login.config", "/Users/vsingh/Software/KerberosLogin.conf");
  }

  public void demoConsumer() {
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("foo", "bar"));
    long initialtimeInMillis = Calendar.getInstance().getTimeInMillis();
    long finaltimeInMillis = initialtimeInMillis + 1000;
    long currenttimeInMillis = Calendar.getInstance().getTimeInMillis();
    while (finaltimeInMillis > currenttimeInMillis) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
      for (ConsumerRecord<byte[], byte[]> record : records) {
        System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
      }

      currenttimeInMillis = Calendar.getInstance().getTimeInMillis();

    }
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {

    SecureAsyncKafkaConsumer secureKafkaConsumerClient = new SecureAsyncKafkaConsumer();
    secureKafkaConsumerClient.setUp();
    secureKafkaConsumerClient.demoConsumer();

  }

}
