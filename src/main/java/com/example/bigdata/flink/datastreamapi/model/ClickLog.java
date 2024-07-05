package com.example.bigdata.flink.datastreamapi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class ClickLog {
    private String user;
    private long ts;
    private String url;
}
