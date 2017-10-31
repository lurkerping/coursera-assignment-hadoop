package com.xplmc.coursera.spark.startup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Simple Join In Spark
 * https://www.coursera.org/learn/hadoop/programming/E46Tk/simple-join-in-spark
 *
 * @author luke
 */
public class SimpleJoinInSpark {

    private static final Logger logger = LoggerFactory.getLogger(SimpleJoinInSpark.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkFun").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load file from disk
        JavaRDD<String> fileA = sc.textFile("file:///F:\\input\\join1_FileA.txt");
        JavaRDD<String> fileB = sc.textFile("file:///F:\\input\\join1_FileB.txt");

        //format: able,991
        JavaPairRDD<String, Integer> fileAPair = fileA.mapToPair((PairFunction<String, String, Integer>) o -> {
            String[] array = o.split(",");
            return new Tuple2<>(array[0], new Integer(array[1]));
        });

        //format: Jan-01 able,5
        JavaPairRDD<String, String> fileBPair = fileB.mapToPair((PairFunction<String, String, String>) o -> {
            String[] array1 = o.split(" ");
            String[] array2 = array1[1].split(",");
            return new Tuple2<>(array2[0], array1[0] + " " + array2[1]);
        });

        logger.info("fileAPair collect: {}", fileAPair.collect());
        logger.info("fileBPair collect: {}", fileBPair.collect());
        logger.info("fileBPair join fileAPair collect: {}", fileBPair.join(fileAPair).collect());


    }

}
