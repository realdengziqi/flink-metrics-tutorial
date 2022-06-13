package com.atguigu.metrics.examples;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>示例3，自定义counter,进行计数(flatmap的并行度设为2)</p>
 *
 * @author : realdengziqi
 * @date : 2022-06-04 00:44
 **/
public class Example03 {
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
                        // 5. 首先要注意！这里使用的是richFunction
                        new RichFlatMapFunction<String, Tuple2<String,Long>>() {
                            // 6. 做好metric的引用
                            Counter counter;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 7.在open方法中
                                //      7.1 通过getRuntimeContext方法拿到
                                //      7.2 调用getMetricGroup()获取指标组
                                //      7.3 调用counter会得到一个新的
                                counter = getRuntimeContext()
                                        .getMetricGroup()
                                        .counter("outCounter");
                            }

                            @Override
                            public void flatMap(String line, Collector<Tuple2<String,Long>> collector) throws Exception {
                                String[] words = line.split(" ");
                                // 8. 进行一次清洗，符合邮箱格式的留下
                                //    不符合的让计数器+1
                                for (String word : words) {
                                    if(isEmail(word)){
                                        collector.collect(Tuple2.of(word,1L));
                                    }else {
                                        counter.inc();
                                    }
                                }
                            }
                        }
                )
                .setParallelism(2)
                .keyBy(data -> data.f0)
                .sum("f1")
                .print();
        env.execute();

    }

    /**
     * 判断输入是否符合email格式 ****@***.***
     * @param s 输入
     * @return 是返回true,否返回false
     */
    public static Boolean isEmail(String s){
        return Pattern.matches("^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$", s);
    }
}
