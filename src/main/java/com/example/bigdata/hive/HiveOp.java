package com.example.bigdata.hive;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 首先要启动hiveserver2
 */
@Slf4j
@Component
public class HiveOp {
    String jdbcUrl = "jdbc:hive2://localhost:10000";
    Connection connection;

    @PostConstruct
    public void init() {
        try {
            connection = DriverManager.getConnection(jdbcUrl, "bytedance", "");
        } catch (Exception e) {
            log.error("init HiveOp failed.", e);
        }
    }

    public void executeSqlQuery(String sql) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            String string = resultSet.toString();
            System.out.println(1);
        } catch (Exception e) {
            log.error("executeSql Hive sql failed,sql=[{}].", sql, e);
        }

    }
}
