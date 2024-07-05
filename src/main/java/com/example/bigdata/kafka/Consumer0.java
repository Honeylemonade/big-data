package com.example.bigdata.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Consumer0 {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Consumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());


//        List<TopicPartition> partitions = new ArrayList<>();
//        partitions.add(new TopicPartition("pk-2-2", 0));
//        partitions.add(new TopicPartition("pk-2-2", 1));

        // 手工指定需要消费的topic对应的partition
        consumer.subscribe(Collections.singletonList("pk-2-2"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                long offset = record.offset();
                int partition = record.partition();
                String topic = record.topic();
                System.out.println(String.format("Consumed0 event from topic=[%s]|partition=[%s]|offset=[%s]: key = [%s] value = [%s]", topic, partition, offset, key, value));
            }
            consumer.commitSync();
        }

    }
}
