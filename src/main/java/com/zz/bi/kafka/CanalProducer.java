package com.zz.bi.kafka;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.zz.bi.util.PropertyUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class CanalProducer {
    final static Logger logger = LoggerFactory.getLogger(CanalProducer.class);
    private final String config;
    private final Properties prop;
    public CanalProducer(String config){
       this.config = config;
       this.prop = PropertyUtils.loadProperties(config);
    }

    public void produce(Supplier<List<ProducerRecord>> supplier) {
        final Producer producer = new KafkaProducer(prop);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            logger.debug("Program is exiting, Producer was stopped.");
        }));
        Stream.generate(supplier).flatMap(l->l.stream()).peek(m->{
            System.out.println(m);
        }).forEach(msg -> producer.send(msg,(m,meta)->{
            logger.debug("send message[{}] to {}, current offset is {}",m.toString(), m.topic(), m.offset());
        }));
    }
}
