package com.atguigu.metrics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * <h4>FlinkMetricTutorial</h4>
 * <p>示例7，直方图</p>
 *
 * @author : realdengziqi
 * @date : 2022-06-05 04:49
 **/
public class Example07 {
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
                            Histogram das;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                das = getRuntimeContext()
                                        .getMetricGroup()
                                        .histogram("das", new DescriptiveStatisticsHistogram(500));
                            }

                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                                das.update(1L);
                                String[] words = line.split(" ");
                                for (String word : words) {
                                    collector.collect(Tuple2.of(word, 1L));
                                }
                            }
                        }
                )
                .keyBy(data -> data.f0)
                .print();
        // 5. 执行之
        env.execute();
    }
}
