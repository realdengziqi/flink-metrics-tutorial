package com.atguigu.metrics;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.stream.FactoryConfigurationError;

/**
 * <h4>示例1.没有添加任何metric的flink程序</h4>
 * <p>
 *
 * </p>
 *
 * @author : realdengziqi
 * @date : 2022-06-03 19:12
 **/
public class Example01 {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 指定nc的host和port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 3. 读取无界流
        DataStreamSource<String> streamSource = env.socketTextStream(hostname, port);

        // 4. 转换数据，对单词进行拆分，转换成(word,1L)
        streamSource
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                                String[] words = line.split(" ");
                                for (String word : words) {
                                    collector.collect(Tuple2.of(word, 1L));
                                }
                            }
                        }
                )
                .keyBy(data -> data.f0)
                .sum("f1")
                .print();
        // 5. 执行之
        env.execute();
    }
}
