package com.atguigu.metrics.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>示例4，meter速率</p>
 *
 * @author : realdengziqi
 * @date : 2022-06-05 04:00
 **/
public class Example04 {
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
                        new RichFlatMapFunction<String, Tuple2<String, Long>>() {
                            // 5. 声明一个引用
                            Meter noEmailMeter;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 6. 在open方法初始化时，将其实例化
                                //      meter内部其实封装的就是counter
                                this.noEmailMeter = getRuntimeContext()
                                        .getMetricGroup()
                                        .meter("noEmailMeter", new MeterView(10));
                            }

                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                                String[] words = line.split(" ");
                                for (String word : words) {
                                    if(isEmail(word)){
                                        collector.collect(Tuple2.of(word,1L));
                                    }else {
                                        // 7.如果不符合email格式，就打个标记
                                        noEmailMeter.markEvent();
                                    }
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

    /**
     * 判断输入是否符合email格式 ****@***.***
     * @param s 输入
     * @return 是返回true,否返回false
     */
    public static Boolean isEmail(String s){
        return Pattern.matches("^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$", s);
    }
}
