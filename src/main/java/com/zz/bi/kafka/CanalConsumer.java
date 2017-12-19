package com.zz.bi.kafka;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zz.bi.canal.MySQLParser;
import com.zz.bi.util.PropertyUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class CanalConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CanalConsumer.class);
    private static final String topic = "canal-test";

    public static void main(String[] args) throws InvalidProtocolBufferException {
        Properties properties = PropertyUtils.loadProperties("consumer.props");
        KafkaConsumer consumer = new KafkaConsumer(properties);
        consume(consumer, topic);
    }

    private static void consume(Consumer consumer, String topic) throws InvalidProtocolBufferException {
        TopicPartition tp = new TopicPartition(topic, 0);
        OffsetAndMetadata om = new OffsetAndMetadata(0);
        Map checkPoint = new HashMap<>(1);
        checkPoint.put(tp, om);
        consumer.commitSync(checkPoint);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println(records);
            /*
            for (ConsumerRecord<String, CanalEntry.Entry> record : records) {
                CanalEntry.Entry entry = record.value();
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                new MySQLParser().processRowChange(rowChange,entry.getHeader());
                logger.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
            }
            */
        }
    }
}
