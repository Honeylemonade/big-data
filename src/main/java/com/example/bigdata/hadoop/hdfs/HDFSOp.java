package com.example.bigdata.hadoop.hdfs;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;

/**
 * 用于操作HDFS
 */
@Component
@Slf4j
public class HDFSOp {

    private FileSystem fileSystem;
    Configuration conf = new Configuration();

    @PostConstruct
    void init() {
        try {
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            fileSystem = FileSystem.get(conf);
        } catch (Exception e) {
            log.error("Failed to init HDFS FileSystem operator", e);
        }
    }

    /**
     * 通过工具类，把文件输入流拷贝到输出流中进行文件上传
     */
    public void uploadFile(String filePath, String targetPath) {
        try {
            FileInputStream ioIn = new FileInputStream(filePath);
            FSDataOutputStream ioOut = fileSystem.create(new Path(targetPath));
            IOUtils.copyBytes(ioIn, ioOut, 1024, false);
            log.info("uploadFile success, targetPath=[{}]", targetPath);
        } catch (Exception e) {
            log.error("uploadFile failed", e);
        }
    }

    public void uploadSequenceFile(String[] inputPaths, String targetPath) {
        try {
            SequenceFile.Writer.Option[] opts = new SequenceFile.Writer.Option[]{
                    SequenceFile.Writer.file(new Path(targetPath)),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(Text.class),
            };
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, opts);

            // 指定需要压缩的文件
            for (String inputPath : inputPaths) {
                File f = FileUtils.getFile(inputPath);
                String content = FileUtils.readFileToString(f, "UTF-8");
                Text key = new Text(f.getName());
                Text value = new Text(content);

                writer.append(key, value);
            }
            writer.close();
        } catch (Exception e) {
            log.error("uploadSequenceFile failed", e);
        }
    }
}
