package com.xplmc.coursera.spark.startup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkFun {

    private static final Logger logger = LoggerFactory.getLogger(SparkFun.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkFun").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> wordList = Arrays.asList("you", "are", "really", "beautiful", "ok", "my", "god", "really", "pretty");
        JavaRDD<String> wordRDD = sc.parallelize(wordList, 3);
        logger.info("wordRDD collect: {}", wordRDD.collect());
        logger.info("wordRDD glom outputs: {}", wordRDD.glom().collect());
        JavaRDD<String> textRDD = sc.textFile("file:///J:\\baidu\\1632\\163-2-01.txt");
        logger.info("first line: {}", textRDD.take(1));
        JavaRDD<String> pwdRDD = textRDD.map((Function<String, String>) v1 -> {
            String[] temp = v1.split("----");
            return temp.length == 2 ? temp[1] : null;
        });
        JavaPairRDD<String, Integer> pairRDD = pwdRDD.mapToPair((PairFunction) o -> new Tuple2(o, 1));
        JavaPairRDD<String, Integer> sumedPairRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        for (Tuple2<String, Integer> tuple : sumedPairRDD.collect()) {
            if (tuple._2 > 100) {
                System.out.println(tuple._1 + "----" + tuple._2);
            }
        }
    }

}
