package com.example.bigdata.flink.datastreamapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class JoinTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Item {
        private long timestamp;
        private Integer groupId;
        private String content;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 8889);
        testTumblingWindowJoin(source1, source2);
        env.execute();
    }

    public static void testTumblingWindowJoin(DataStreamSource<String> source1, DataStreamSource<String> source2) {
        SingleOutputStreamOperator<Item> s1 = source1.map(JoinTest::strToItem)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Item>forMonotonousTimestamps()
                        .withTimestampAssigner((item, timestamp) -> item.getTimestamp()));

        SingleOutputStreamOperator<Item> s2 = source2.map(JoinTest::strToItem)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Item>forMonotonousTimestamps()
                        .withTimestampAssigner((item, timestamp) -> item.getTimestamp()));

        s1.join(s2)
                //第一个流的key
                .where(Item::getGroupId)
                //第二个流的key
                .equalTo(Item::getGroupId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply((JoinFunction<Item, Item, String>) (first, second) -> {
                    return first.toString() + "&" + second.toString();
                })
                .print();
    }

    public static Item strToItem(String str) {
        String[] strings = str.split(",");
        return new Item(Long.parseLong(strings[0]), Integer.valueOf(strings[1]), strings[2]);
    }
}
