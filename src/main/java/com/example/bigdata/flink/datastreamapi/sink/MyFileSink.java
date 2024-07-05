package com.example.bigdata.flink.datastreamapi.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class MyFileSink {
    public static String path = "/Users/bytedance/IdeaProjects/big-data/src/main/resources/output.log";

    public static FileSink<String> getMyFilesink() {
        return FileSink.forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build();
    }
}
