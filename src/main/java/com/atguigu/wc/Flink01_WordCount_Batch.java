package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author shkstart
 * @create 2020-11-16 14:44
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件中读取数据
        String inputPath = "hello.txt";
        DataSource<String> lineDS = env.readTextFile(inputPath);

        //3.压平操作
        FlatMapOperator<String, Tuple2<String, Integer>> wordsToOneDS = lineDS.flatMap(new MyFlatMapFunc());

        //4.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordsToOneDS.groupBy(0);

        //5.聚合计算
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);

        //6.打印结果
        result.print();


    }

    //自定义FlatMapFunction
    private static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按照空格切分value
            String[] words = value.split(" ");
            //遍历words输出数据
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
