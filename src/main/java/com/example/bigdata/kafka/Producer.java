package com.example.bigdata.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        /* Thread.sleep(100); */


        while (true) try {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("pkflink", new Random().nextInt(1), null, genClickJson());

            Future<RecordMetadata> send = producer.send(producerRecord, (recordMetadata, e) -> {
                System.out.printf("完成消息发送|partition=[%s]|offset=[%s]%n", recordMetadata.partition(), recordMetadata.offset());
            });
            send.get(2000L, TimeUnit.MILLISECONDS);
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("send 失败");
        }
    }

    static int count = 1;

    public static String genClickJson() throws JsonProcessingException {
        Map<String, Integer> map = new HashMap<>();
        map.put("user", count++);

        ObjectMapper objectMapper = new ObjectMapper();
        String result = objectMapper.writeValueAsString(map);
        System.out.println(result);
        return result;
    }
}
