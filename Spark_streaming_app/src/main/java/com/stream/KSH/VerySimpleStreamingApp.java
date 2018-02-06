package com.stream.KSH;

import java.util.*;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.*;
import org.json.JSONObject;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class VerySimpleStreamingApp {


    public static void main(String[] args) throws Exception {

        /*Set kafka parameters*/
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.1.33:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        /*set spark stream context*/
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaToHbase")
                .setMaster("local")
                .set("spark.shuffle.blockTransferService", "nio");
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf,
                Durations.seconds(2));
        Collection<String> topics = Arrays.asList("topic_project");

        /*Init streaming*/
        JavaDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        /*Send the messages to each column for each log received*/
        stream.foreachRDD(rdd -> {
            rdd.foreachPartition(consumerRecords -> {
                System.out.println("READING RDD");
                if (consumerRecords.hasNext()){
                    //Init
                    Connection connection = null;
                    Table table = null;
                    List<Put> puts = new ArrayList<>();
                    final TableName TABLE_NAME = TableName.valueOf("Tlogs");
                    final byte[] CF = Bytes.toBytes("CF");
                            /*set Hbase configuration*/
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", "localhost");
                    conf.set("hbase.zookeeper.property.clientPort", "2181");

                    // establish the connection to the cluster.
                    connection = ConnectionFactory.createConnection(conf);
                    // retrieve a handle to the target table.
                    table = connection.getTable(TABLE_NAME);

                    String line =consumerRecords.next().value();
                    JSONObject jobj = new JSONObject(line);

                    /*Print the logs*/
                    System.out.println(jobj.get("@timestamp").toString());
                    System.out.println(jobj.get("@version").toString());
                    System.out.println(jobj.get("message").toString());
                    System.out.println(jobj.get("logger_name").toString());
                    System.out.println(jobj.get("thread_name").toString());
                    System.out.println(jobj.get("level").toString());
                    System.out.println(jobj.get("level_value").toString());
                    System.out.println(jobj.get("HOSTNAME").toString());
                    // describe the data we want to write.
                    Put p1 = new Put(Bytes.toBytes(jobj.get("@timestamp").toString()));
                    Put p2 = new Put(Bytes.toBytes(jobj.get("@version").toString()));
                    Put p3 = new Put(Bytes.toBytes(jobj.get("message").toString()));
                    Put p4 = new Put(Bytes.toBytes(jobj.get("logger_name").toString()));
                    Put p5 = new Put(Bytes.toBytes(jobj.get("thread_name").toString()));
                    Put p6 = new Put(Bytes.toBytes(jobj.get("level").toString()));
                    Put p7 = new Put(Bytes.toBytes(jobj.get("level_value").toString()));
                    Put p8 = new Put(Bytes.toBytes(jobj.get("HOSTNAME").toString()));
                    p1.addColumn(CF, Bytes.toBytes("@timestamp"), Bytes.toBytes(line));
                    p2.addColumn(CF, Bytes.toBytes("@version"), Bytes.toBytes(line));
                    p3.addColumn(CF, Bytes.toBytes("message"), Bytes.toBytes(line));
                    p4.addColumn(CF, Bytes.toBytes("logger_name"), Bytes.toBytes(line));
                    p5.addColumn(CF, Bytes.toBytes("thread_name"), Bytes.toBytes(line));
                    p6.addColumn(CF, Bytes.toBytes("level"), Bytes.toBytes(line));
                    p7.addColumn(CF, Bytes.toBytes("level_value"), Bytes.toBytes(line));
                    p8.addColumn(CF, Bytes.toBytes("HOSTNAME"), Bytes.toBytes(line));
                    puts.add(p1);
                    puts.add(p2);
                    puts.add(p3);
                    puts.add(p4);
                    puts.add(p5);
                    puts.add(p6);
                    puts.add(p7);
                    puts.add(p8);
                    table.put(puts);
                    // close everything down
                    if (table != null) {
                        table.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }

                }else {
                    System.out.println("NO RDD provided");
                }


            });
        });




        // Execute the Spark workflow defined above
        streamingContext.start();
        streamingContext.awaitTermination();
    }

}