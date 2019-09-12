package com.youyuan.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @author zhangyu
 * @description 指定patition
 * @date 2018/10/15 23:02
 */
public class CustomPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 控制分区
		return 1;
	}

	@Override
	public void close() {
		
	}
}


