package com.example.bigdata.flink.datastreamapi.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestCheckpoint {
    public static void main(String[] args) throws Exception {
        testKeyedValueState();
    }

    public static void testKeyedValueState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkPoint配置
        env.enableCheckpointing(5000); // 每隔5秒执行一次checkPoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 仅执行一次
        env.getCheckpointConfig().setCheckpointTimeout(60000); // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 同一时间只允许进行一个 checkpoint
//        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoint"));
        env.setStateBackend(new FsStateBackend("file:///Users/bytedance/IdeaProjects/big-data/checkpoints"));
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
}
