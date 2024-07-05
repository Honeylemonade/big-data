package com.example.bigdata.flink.datastreamapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("/Users/bytedance/IdeaProjects/big-data/src/main/resources/words")).build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "A file source")
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] strings = s.split(",");
                    for (String string : strings) {
                        collector.collect(new Tuple2<>(string, 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(t -> t.f0)
                .sum(1)// 加tuple的第二个元素，下标为1
                .rebalance()
                .sinkTo(new PrintSink<>());

        env.execute("流式统计单词数");
    }

}
