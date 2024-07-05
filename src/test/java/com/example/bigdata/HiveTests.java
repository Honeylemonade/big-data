package com.example.bigdata;

import com.example.bigdata.hive.HiveOp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class HiveTests {
    @Autowired
    HiveOp hiveOp;

    @Test
    public void test() {
        hiveOp.executeSqlQuery("select * from user_info;");
    }
}

