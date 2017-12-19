package com.zz.bi.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class SimpleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private static final String topic = "canal-test";

    public static void main(String[] args) {
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            KafkaConsumer consumer = new KafkaConsumer(properties);
            consume(consumer, topic);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void consume(Consumer consumer, String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        OffsetAndMetadata om = new OffsetAndMetadata(1);
        Map checkPoint = new HashMap<>(1);
        checkPoint.put(tp,om);
        consumer.commitSync(checkPoint);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }
        }
    }
}
