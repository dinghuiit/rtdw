package com.zz.bi.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CanalLogSupplier implements Supplier<List<ProducerRecord>>{

    private static final Logger logger = LoggerFactory.getLogger(CanalLogSupplier.class);
    private static final int BATCH_SIZE = 1000; //每次处理的记录数
    private String topic_filter = ".*\\..*"; //订阅的事件主题
    final CanalConnector connector;

    public CanalLogSupplier(String ZK, String destination, String username, String password) {
        connector = CanalConnectors.newClusterConnector(ZK, destination, username, password);
        connector.connect();
        connector.subscribe(topic_filter);
        connector.rollback();
    }

    @Override
    public List<ProducerRecord> get() {
        while (true){
            Message message = connector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
            List<Entry> rows = message.getEntries();
            long batchId = message.getId();
            if (rows.size() == 0 || batchId == -1) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            connector.ack(batchId);
            return rows.stream().map(e->e.getHeader().getTableName()).map(e->new ProducerRecord("canal-test",e)).collect(Collectors.toList());
        }
    }
}
