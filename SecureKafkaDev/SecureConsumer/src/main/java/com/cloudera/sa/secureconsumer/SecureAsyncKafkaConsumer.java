package com.cloudera.sa.secureconsumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

  public void setUp() throws UnknownHostException {
    props.put("bootstrap.servers", "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092,"
        + "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092");
    props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("client.id", this.getClass().getName() + "-" + InetAddress.getLocalHost().getHostName());
    
    props.put("group.id", "DemoTest");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    System.setProperty("java.security.auth.login.config", "/Users/vsingh/Software/KerberosLogin.conf");
  }

  public void demoConsumer() {
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("topic1"));
    long initialtimeInMillis = Calendar.getInstance().getTimeInMillis();
    long finaltimeInMillis = initialtimeInMillis + 1000;
    long currenttimeInMillis = Calendar.getInstance().getTimeInMillis();
    while (true) {
      ConsumerRecords<Integer, String> records = consumer.poll(100);
      for (ConsumerRecord<Integer, String> record : records) {
        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
      }

      currenttimeInMillis = Calendar.getInstance().getTimeInMillis();

    }
    
  }

  /**
   * @param args the command line arguments
   * @throws java.net.UnknownHostException
   */
  public static void main(String[] args) throws UnknownHostException {

    SecureAsyncKafkaConsumer secureKafkaConsumerClient = new SecureAsyncKafkaConsumer();
    secureKafkaConsumerClient.setUp();
    secureKafkaConsumerClient.demoConsumer();

  }

}
