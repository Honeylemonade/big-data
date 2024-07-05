package com.example.bigdata.flink.datastreamapi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
@AllArgsConstructor
public class User {
    private Integer userId;
    private String name;
    private Integer age;
}
