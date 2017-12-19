package com.zz.bi.spark;

import com.zz.bi.util.PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

@SuppressWarnings("unchecked")
public class SparkDemo {
    public static void main(String[] args) throws InterruptedException {
        String topic = "canal-test";
        Collection<String> topics = Arrays.asList(topic);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("canal-streaming");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        Properties properties = PropertyUtils.loadProperties("streaming-consumer.props");
        Map<String,Object> param = new HashMap(properties);

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(topic, 0), 0L);

        final JavaInputDStream<ConsumerRecord<Object, Object>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, param, offsets));

        stream.foreachRDD(rdd->{
            rdd.foreach(r->{
                System.out.println(r.value());
            });
        });
        ssc.start();
        ssc.awaitTermination();
    }
}

