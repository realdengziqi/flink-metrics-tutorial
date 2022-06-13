package com.atguigu.metrics.examples;

import com.sun.java.swing.plaf.windows.resources.windows;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.util.Collector;

import java.util.regex.Pattern;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>示例5，瞬时值/p>
 *
 * @author : realdengziqi
 * @date : 2022-06-05 03:35
 **/
public class Example05 {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 指定nc的host和port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host","hadoop102");
        int port = parameterTool.getInt("port",9999);

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
                                    collector.collect(Tuple2.of(word,1L));
                                }
                            }
                        }
                )
                .keyBy(data -> data.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Tuple2<String, Long> record, Long aLong) {
                                return record.f1 + aLong;
                            }

                            @Override
                            public Long getResult(Long aLong) {
                                return aLong;
                            }

                            @Override
                            public Long merge(Long aLong, Long acc1) {
                                return null;
                            }
                        },
                        new RichWindowFunction<Long, Tuple2<String,Long>, String, TimeWindow>() {

                            Long maxWindowSize = 0L;
                            Gauge<Long> gauge;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                gauge = getRuntimeContext()
                                        .getMetricGroup()
                                        .gauge("maxWindowSize", new Gauge<Long>() {
                                            @Override
                                            public Long getValue() {
                                                return maxWindowSize;
                                            }
                                        });
                            }

                            @Override
                            public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                                System.out.println("被调用一次");
                                for (Long aLong : iterable) {
                                    System.out.println("dasda");
                                    collector.collect(Tuple2.of(key,aLong));
                                }
                            }
                        }
                )
                .print();
        // 5. 执行之
        env.execute();
    }


}
