CREATE TABLE kafka_table
(
    `user` BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'pk-2-2',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'testGroup',
--       'scan.startup.mode' = 'latest-offset',
      'scan.startup.mode' = 'group-offsets',
      'properties.auto.offset.reset' = 'earliest',
      'format' = 'json'
)