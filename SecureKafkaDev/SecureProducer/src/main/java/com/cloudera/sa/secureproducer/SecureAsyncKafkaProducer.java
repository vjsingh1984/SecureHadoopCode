/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.secureproducer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author vsingh
 */
public class SecureAsyncKafkaProducer {

  Properties props = new Properties();
  Properties propsKerberos ;
  

  public void setUp() throws UnknownHostException, IOException {
    
    props.put("client.id", this.getClass().getName() + "-" + InetAddress.getLocalHost().getHostName());
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("bootstrap.servers", "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092,"+
        "SASL_PLAINTEXT://ip-10-20-0-5.us-west-2.compute.internal:9092");
    props.put("security.protocol","SASL_PLAINTEXT");
    
    // Setup Kerberos Properites
    Properties propsKerberos = System.getProperties();
    propsKerberos.put("java.security.auth.login.config", "/Users/vsingh/Software/KerberosLogin.conf");
    System.setProperties(propsKerberos);

  }
  

  public void demoProducer() {
    
    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 10; i++) {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic1", Bytes.toBytes(i), Bytes.toBytesBinary("Message-" + i));

      producer.send(record/*,
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata meta, Exception e) {
              if (e != null) {
                e.printStackTrace();
              } else {
                System.out.println("Msg Successful Ack: [Offset,Partition]=[" + meta.offset() + "," + meta.partition() + "]");
              }
            }
          }*/);
    }
  }

  public static void main(String[] args) throws UnknownHostException, IOException {
    SecureAsyncKafkaProducer secureKafkaProducerClient = new SecureAsyncKafkaProducer();
    secureKafkaProducerClient.setUp();

    secureKafkaProducerClient.demoProducer();

  }

}
