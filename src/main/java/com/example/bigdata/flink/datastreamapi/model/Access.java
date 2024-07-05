package com.example.bigdata.flink.datastreamapi.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown=true)
public class Access implements Serializable {
    private int category;
    private String description;
    private int id;
    private String ip;
    private int money;
    private String name;
    private String os;
    private int statues;
    private long ts;
}
