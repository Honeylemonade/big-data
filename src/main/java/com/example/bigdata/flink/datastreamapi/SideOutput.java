package com.example.bigdata.flink.datastreamapi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final OutputTag<Double> outputTag = new OutputTag<Double>("Even number side-output") {};
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);

        SingleOutputStreamOperator<Integer> process = source.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
                if (value % 2 == 0) {
                    ctx.output(outputTag, Double.valueOf(value));
                }
            }
        });
        process.addSink(new PrintSinkFunction<>());

        SideOutputDataStream<Double> sideOutput = process.getSideOutput(outputTag);
        sideOutput.addSink(new PrintSinkFunction<>());

        env.execute();
    }
}
