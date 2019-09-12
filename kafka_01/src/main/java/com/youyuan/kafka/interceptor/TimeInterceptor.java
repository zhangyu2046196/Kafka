package com.youyuan.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zhangyu
 * @version 1.0
 * @description 自定义拦截器,应用场景在producer生产消息发送到kafka之前将消息之前加入时间戳
 * @date 2018/10/17 21:26
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    /**
     * 消息发送到kafka之前处理消息
     * @param producerRecord
     * @return
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String,String>(producerRecord.topic(),producerRecord.partition(),producerRecord.key(),System.currentTimeMillis()+","+producerRecord.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }
    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
