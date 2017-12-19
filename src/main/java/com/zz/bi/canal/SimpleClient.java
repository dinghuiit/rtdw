package com.zz.bi.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleClient {

    private static final int RETRY_TIMES = 10; //失败重试次数，超过重试次数后客户端将退出
    private static final int BATCH_SIZE = 1000; //每次处理的记录数
    private static final String CANAL_HOST = "20.1.1.11";
    private static final String ZK = "localhost:2181";
    private static final int CANAL_PORT = 17611;
    private static final Logger logger = LoggerFactory.getLogger(SimpleClient.class);
    private String destination = "test";
    private String topic_filter = "test\\..*"; //订阅的事件主题
    private String username = "";
    private String password = "";

    public static void main(String[] args) {
        new SimpleClient().start();
    }

    public SimpleClient() {
    }

    public SimpleClient(String destination, String topic_filter, String username, String password) {
        this.destination = destination;
        this.topic_filter = topic_filter;
        this.username = username;
        this.password = password;
    }

    public void start() {
        InetSocketAddress address = new InetSocketAddress(CANAL_HOST, CANAL_PORT);
//        CanalConnector connector = CanalConnectors.newSingleConnector(address, destination, username, password);
        CanalConnector connector = CanalConnectors.newClusterConnector(ZK, destination, username, password);
        int retry = 0;
        try {
            connector.connect();
            connector.subscribe(topic_filter);
            connector.rollback();
            while (retry < RETRY_TIMES) {
                Message message = connector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
                long batchId = message.getId();
                List<Entry> rows = message.getEntries();
                if (rows.size() == 0 || batchId == -1) {
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                try {
                    logger.debug("start to process binlog...");
                    processBatchRows(rows);
                    connector.ack(batchId);
                    retry = 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    connector.rollback();
                    retry++;
                    logger.debug("somethine went wrong, rollback, retry  at {} ", retry);
                }
            }
            logger.error("somethine went wrong, retry after {} times, will exit!", retry);
        } finally {
            connector.disconnect();
        }
    }

    /**
     * 解析一个binlog事件
     *
     * @param rows see EntryProtocol.proto
     */

    private static void processBatchRows(List<Entry> rows) {
        final CanalParser parser = new MySQLParser();
        rows.parallelStream()
                .filter(row -> row.getEntryType() != EntryType.TRANSACTIONBEGIN)
                .filter(row -> row.getEntryType() != EntryType.TRANSACTIONEND)
                .forEach(row -> {
                    try {
                        RowChange rowChange = RowChange.parseFrom(row.getStoreValue());
                        CanalEntry.Header header = row.getHeader();
                        parser.processRowChange(rowChange, header);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                });
    }
}
