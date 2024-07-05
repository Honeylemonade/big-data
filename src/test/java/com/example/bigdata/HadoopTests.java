package com.example.bigdata;

import com.example.bigdata.hadoop.hdfs.HDFSOp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class HadoopTests {

    @Autowired
    private HDFSOp hdfsOp;

    @Test
    public void test() {
        hdfsOp.uploadFile("/Users/bytedance/Desktop/未命名.txt", "/new_未命名.txt");
    }

    @Test
    public void test2() {
        String[] paths = {"/Users/bytedance/Desktop/small/未命名.txt",
                "/Users/bytedance/Desktop/small/未命名_副本.txt",
                "/Users/bytedance/Desktop/small/未命名_副本2.txt",
                "/Users/bytedance/Desktop/small/未命名_副本3.txt",
                "/Users/bytedance/Desktop/small/未命名_副本4.txt",
                "/Users/bytedance/Desktop/small/未命名_副本5.txt",
                "/Users/bytedance/Desktop/small/未命名_副本6.txt",
                "/Users/bytedance/Desktop/small/未命名_副本7.txt",
                "/Users/bytedance/Desktop/small/未命名_副本8.txt",
                "/Users/bytedance/Desktop/small/未命名_副本9.txt",
                "/Users/bytedance/Desktop/small/未命名_副本10.txt"
        };
        hdfsOp.uploadSequenceFile(paths, "/seqFile");
    }
}
