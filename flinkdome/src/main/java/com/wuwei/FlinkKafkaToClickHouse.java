package com.wuwei;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.types.Row;

import java.util.Properties;

public class FlinkKafkaToClickHouse {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 设置kafka参数
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // 指定kafka地址端口
                .setBootstrapServers("hadoop102:9002,hadoop103:9002")
                .setGroupId("wuwei")
                .setTopics("Topic1")
                //设置反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 默认earliest
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 开启kafka底层消费者的自动位移提交，会把最新的消费位移提交到kafka的consumer_offsets中
                // 就算开启自动位移提交机制，kafkaSource依然不依赖自动位移提交机制（宕机重启时优先从flink自己的状态中获取偏移量）
                //.setProperty("auto.offset.commit", "true")
                .build();
/*
  kafka消费者参数
       auto.reset.offsets
           earliest: 如果有offset 则从offset消费；如果没有从 最早 消费
           latest  : 如果有offset 则从offset消费；如果没有从 最新 消费
 */
        env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkasource").setParallelism(2)
                .print();

        // 写入ClickHouse
//        tableEnv.fromDataStream(rowDataStream)
//                .insertInto("jdbc-connector://<your_clickhouse_url>?user=<username>&password=<password>", "your_table");
//
//        env.execute("FlinkKafkaToClickHouse");
    }
}