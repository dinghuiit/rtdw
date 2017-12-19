package com.zz.bi;

import com.zz.bi.canal.CanalLogSupplier;
import com.zz.bi.kafka.CanalProducer;

public class Main {
    public static void main(String[] args) {
        CanalLogSupplier supplier = new CanalLogSupplier("localhost:2181","test", "", "");
        new CanalProducer("producer.props").produce(supplier);
    }
}
