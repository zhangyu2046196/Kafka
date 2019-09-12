package com.youyuan.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zhangyu
 * @version 1.0
 * @description 数量拦截器，应用场景在producer发送消息给kafka后记录消息发送成功和失败的数量
 * @date 2018/10/17 21:36
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
    private Long successCount=0l;//消息发送成功数量
    private Long failCount=0l;//消息发送失败数量

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return null;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e==null){
            successCount++;
        }else {
            failCount++;
        }
    }

    public void close() {
        System.out.println("消息发送成功数量:"+successCount);
        System.out.println("消息发送失败数量:"+failCount);
    }

    public void configure(Map<String, ?> map) {

    }
}
