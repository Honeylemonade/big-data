package com.example.bigdata.flink.datastreamapi;

import com.example.bigdata.flink.datastreamapi.model.ClickLog;
import com.google.common.collect.Iterables;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<ClickLog> source = env.fromElements(
                new ClickLog("u1", 1000, "url1"),
                new ClickLog("u2", 2000, "url2"),
                new ClickLog("u3", 3000, "url3"),
                new ClickLog("u5", 5000, "url5"),
                new ClickLog("u6", 6000, "url6"),
                new ClickLog("u7", 7000, "url7"),
                new ClickLog("u8", 8000, "url8"),
                new ClickLog("u4", 4000, "url4"),
                new ClickLog("u9", 9000, "url9")
        );
//        tumblingProcessingTimeWindows(source, env);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 8888);
        tumblingProcessingTimeWindows2(source2, env);
        env.execute();
    }

    public static void tumblingProcessingTimeWindows(DataStreamSource<ClickLog> source, StreamExecutionEnvironment env) {
        // 指定watermark
        source.assignTimestampsAndWatermarks(WatermarkStrategy.<ClickLog>forBoundedOutOfOrderness(Duration.ofMillis(200000))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTs()))
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
                .process(new ProcessAllWindowFunction<ClickLog, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<ClickLog, String, TimeWindow>.Context context, Iterable<ClickLog> elements, Collector<String> out) throws Exception {
                        System.out.println("当前窗口元素个数:" + Iterables.size(elements));
                        for (ClickLog element : elements) {
                            System.out.println(element.toString());
                        }
                        out.collect("窗口：[" + context.window().getStart() + "," + context.window().getEnd() + ")");
                    }
                })
                .print();
    }

    public static void tumblingProcessingTimeWindows2(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        // 指定2ms watermark
        source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(2))
                        .withTimestampAssigner((event, recordTimestamp) -> Long.parseLong(event)))
                // 5ms的滚动窗口
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                // 允许迟到5秒
                .allowedLateness(Time.milliseconds(5000))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println("当前窗口元素个数:" + Iterables.size(elements));
                        for (String element : elements) {
                            System.out.println(element);
                        }
                        out.collect("窗口：[" + context.window().getStart() + "," + context.window().getEnd() + ")");
                    }
                })
                .print();
    }
}
