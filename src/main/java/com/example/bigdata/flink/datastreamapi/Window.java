package com.example.bigdata.flink.datastreamapi;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Window {
    /**
     * 通过监听netcat输入的字符串进行窗口聚合
     * nc -k -l 8888
     */
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        processingTimeSessionWindows(source, env);
        env.execute();
    }


    public static void countWindow(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(Integer::parseInt)
                .countWindowAll(5)
                .sum(0)
                .map(String::valueOf)
                .print();
    }

    /**
     * 输入 <String,Integer>>
     *
     * @param source
     * @param env
     */
    public static void countWindowWithKeyBy(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(e -> {
                    String[] strings = e.split(",");
                    return Tuple2.of(strings[0], Integer.parseInt(strings[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0)
                .countWindow(5)
                .sum(1)
                .print();
    }

    public static void tumblingProcessingTimeWindows(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(Integer::parseInt)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0)
                .map(String::valueOf)
                .print();
    }

    public static void tumblingProcessingTimeWindowsWithKeyBy(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(e -> {
                    String[] strings = e.split(",");
                    return Tuple2.of(strings[0], Integer.parseInt(strings[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(e -> e.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
    }

    public static void slidingWindow(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(Integer::parseInt)
                // 每一秒统计过去5秒的数据
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .sum(0)
                .map(String::valueOf)
                .print();
    }


    public static void processingTimeSessionWindows(DataStreamSource<String> source, StreamExecutionEnvironment env) {
        source.map(Integer::parseInt)
                // 5s内无数据，就结束一次会话，开始进行一次清算
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();
    }

}
