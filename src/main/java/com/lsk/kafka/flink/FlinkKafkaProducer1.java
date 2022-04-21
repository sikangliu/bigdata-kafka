package com.lsk.kafka.flink;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Description
 * @Author sikang.liu
 * @Date 2022-04-21 17:34
 */
public class FlinkKafkaProducer1 {
    public static void main(String[] args) throws Exception {
        // 0 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 1 读取集合中数据
        ArrayList<String> wordsList = new ArrayList<>();
        wordsList.add("hello");
        wordsList.add("world");
        DataStream<String> stream = env.fromCollection(wordsList);

        // 2 kafka 生产者配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        
        // 3 创建 kafka 生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "first",
                new SimpleStringSchema(),
                properties
        );

        // 4 生产者和 flink 流关联
        stream.addSink(kafkaProducer);
        // 5 执行
        env.execute();
    }
}
