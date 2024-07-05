package com.example.bigdata.flink.datastreamapi.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManualWordCount {
    public static void main(String[] args) throws Exception {
        testOperatorValueState();
    }

    public static void test1() throws Exception {
        Map<String, Integer> cache = new HashMap<>();

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        source.map(String::toLowerCase)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    for (String word : value.split(",")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(t -> t.f0)
                .map(e -> {
                    cache.put(e.f0, cache.getOrDefault(e.f0, 0) + 1);
                    return e.f0 + ":" + cache.get(e.f0);
                })

                .print();

        env.execute("Manual WordCount");
    }

    public static void testKeyedValueState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.toLowerCase().split(",")) {
                            out.collect(word);
                        }
                    }
                })
                .keyBy(String::toString)
                .flatMap(new RichFlatMapFunction<String, String>() {
                    private transient ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                                "totalCount", // the state name
                                TypeInformation.of(new TypeHint<Integer>() {
                                }), // type information
                                0); // default value of the state, if nothing was set
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        Integer count = state.value();
                        state.update(count + 1);
                        out.collect(value + ":" + state.value());
                    }
                }).print();
        env.execute("Manual WordCount");
    }

    /**
     * 每个算子子任务或者说每个算子实例共享一个状态
     */
    public static void testOperatorValueState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);
        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String word : value.toLowerCase().split(",")) {
                            out.collect(word);
                        }
                    }
                })
                .keyBy(String::toString)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        out.collect(new Tuple2<>(value, 1L));
                    }
                })
                .addSink(new MySink());
        env.execute("Manual WordCount");
    }

    public static class MySink implements SinkFunction<Tuple2<String, Long>>, CheckpointedFunction {
        private ListState<Tuple2<String, Long>> listState;
        private List<Tuple2<String, Long>> bufferedElements = new ArrayList<>();

        /**
         * 算子初始化时执行
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>(
                    "bufferedSinkState",
                    TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                    }));
            listState = context.getOperatorStateStore().getListState(descriptor);
            //检查当前的执行是否是从之前的执行中恢复的状态。
            //具体来说,在你提供的initializeState方法中,代码会检查当前的执行上下文是否是从之前的状态中恢复的。
            if (context.isRestored()) {
                for (Tuple2<String, Long> element : listState.get()) {
                    bufferedElements.add(element);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            for (Tuple2<String, Long> element : bufferedElements) {
                listState.add(element);
            }
        }

        @Override
        public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
            bufferedElements.add(value);
            System.out.println("算子被触发(invoke),数据写入buffer>>" + value);
            for (Tuple2<String, Long> element : bufferedElements) {
                System.out.println("["+Thread.currentThread().getId() + "] >> " + element.f0 + " : " + element.f1);
            }
        }


    }
}
