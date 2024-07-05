package com.example.bigdata.flink.datastreamapi;

import com.example.bigdata.flink.datastreamapi.model.User;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 使用flink异步IO操作MySQL数据
 */
public class AsyncIO {

    public static class AsyncDatabaseRequest extends RichAsyncFunction<String, User> {
        public CompletableFuture<User> sendReq() throws InterruptedException {
            // 模拟client发送请求的延迟,请求返回User对象
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(new Random().nextInt(3000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return new User(1, "name", 23);
            });
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<User> resultFuture) throws Exception {
            CompletableFuture<User> future = sendReq();
            CompletableFuture.supplyAsync(() -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).thenAccept(user -> {
                user.setName(user.getName() + input);
                user.setAge(user.getAge() + 10);
                resultFuture.complete(Collections.singleton(user));
            });
        }
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, // 尝试重启的次数
                Time.of(1, TimeUnit.SECONDS) // 间隔
        ));
        DataStream<String> stream = env.fromElements("a", "b", "c", "d", "e");
        AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 5000, TimeUnit.MILLISECONDS, 100)
                .rebalance()
                .sinkTo(new PrintSink<>());
        env.execute();
    }
}
