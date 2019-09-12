package com.youyuan.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author zhangyu
 * @description 生产者
 * @date 2018/10/15 23:02
 */
public class CustomProducer {
	public static void main(String[] args) {
		System.out.println("测试生产者");
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put("bootstrap.servers", "172.18.32.16:19092");
		// 等待所有副本节点的应答
		props.put("acks", "all");
		// 消息发送最大尝试次数
		props.put("retries", 0);
		// 一批消息处理大小
		props.put("batch.size", 16384);
		// 请求延时
		props.put("linger.ms", 1);
		// 发送缓存区内存大小
		props.put("buffer.memory", 33554432);
		// key序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		//给producer注册拦截器，此处注册拦截器链
		List<String> inList=new ArrayList<String>();
		inList.add("com.youyuan.kafka.interceptor.CountInterceptor");
		inList.add("com.youyuan.kafka.interceptor.TimeInterceptor");

		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,inList);

		System.out.println("props:"+props);

		KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
		for (int i = 0; i < 50; i++) {
			System.out.println("发送消息前....."+i);

			ProducerRecord<String,String> producerRecord= new ProducerRecord<String, String>("myt2", "", "hello world-" + i);
			System.out.println("producerRecord"+producerRecord);
			System.out.println("producer"+producer);
			producer.send(producerRecord);
			System.out.println("发送消息后....."+i);
		}

		producer.close();
	}
}

