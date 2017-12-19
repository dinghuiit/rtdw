package com.zz.bi.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

public class SimpleProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String topic = "canal-test";
    private static final int partitionNum = 0;

    public static void main(String[] args) {
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            final Producer producer = new KafkaProducer(properties);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.close();
                logger.debug("Program is exiting, Producer was stopped.");
            }));
            Stream.generate(() -> {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return new Random().nextInt();
            }).forEach(msg -> send(producer, msg));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void send(Producer producer, Integer msg) {
        String msgId = UUID.randomUUID().toString();
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, partitionNum, msgId, String.valueOf(msg));
        producer.send(producerRecord,
                (meta, e) -> logger.debug("Message[{}] was successfully sent to partition[{}].", msgId, meta.partition()));
    }

}
