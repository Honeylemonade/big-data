package com.example.bigdata.flink.datastreamapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 消费kafka，extractly once 精确一次语义
 */
public class FlinkKafkaSource {
    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topic = "pkflink";
        String groupId = "pkgroup";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .print(); 
        env.execute("EOSApp1");
    }
}
