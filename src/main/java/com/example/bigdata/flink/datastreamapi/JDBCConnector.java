package com.example.bigdata.flink.datastreamapi;

import com.example.bigdata.flink.datastreamapi.model.User;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JDBCConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(new User(1, "t1", 11),
                        new User(2, "t2", 22),
                        new User(3, "t3", 33))
                .addSink(JdbcSink.sink(
                        "insert into user (user_id, name, age) values (?,?,?)",
                        (ps, t) -> {
                            ps.setInt(1, t.getUserId());
                            ps.setString(2, t.getName());
                            ps.setInt(3, t.getAge());
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/xyp_test")
                                .withDriverName("com.mysql.cj.jdbc.Driver") // JDBC 驱动
                                .withUsername("root") // 用户名
                                .withPassword("qwe159852") // 密码
                                .build()));
        env.execute();
    }
}
