package com.xplmc.coursera.spark.startup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Advanced Join In Spark
 * https://www.coursera.org/learn/hadoop/programming/3o9hw/advanced-join-in-spark
 *
 * @author luke
 */
public class AdvancedJoinInSpark {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedJoinInSpark.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkFun").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load file from disk1
        JavaRDD<String> showViewsFile = sc.textFile("file:///F:\\input\\join2_gennum?.txt");
        JavaRDD<String> showChannelFile = sc.textFile("file:///F:\\input\\join2_genchan?.txt");

        //test if loads ok
        logger.info("showViewsFile: {}", showViewsFile.take(2).toString());
        logger.info("showChannelFile: {}", showChannelFile.take(2).toString());

        //format: show name,viewers
        JavaPairRDD<String, Integer> showViews = showViewsFile.mapToPair((PairFunction<String, String, Integer>) o -> {
            String[] array = o.split(",");
            return new Tuple2<>(array[0], new Integer(array[1]));
        });

        //format: show name,channel
        JavaPairRDD<String, String> showChannel = showChannelFile.mapToPair((PairFunction<String, String, String>) o -> {
            String[] array = o.split(",");
            return new Tuple2<>(array[0], array[1]);
        });

        //test if transformation ok
        logger.info("showViews kv: {}", showViews.take(1).toString());
        logger.info("showChannel kv: {}", showChannel.take(1).toString());

        //join
        JavaPairRDD<String, Tuple2<Integer, String>> showInfo = showViews.join(showChannel);

        //test if join ok
        logger.info("views join channel: {}", showInfo.take(1).toString());

        //transformation from show info to channel views
        JavaPairRDD<String, Integer> channelViews = showInfo.mapToPair((PairFunction<Tuple2<String, Tuple2<Integer, String>>, String, Integer>) o ->
                new Tuple2<>(o._2._2, o._2._1));

        //test if transformation ok
        logger.info("channel views: {}", channelViews.take(1).toString());

        //count by channel
        JavaPairRDD<String, Integer> channelViewsByKey = channelViews.reduceByKey((Function2<Integer, Integer, Integer>) (o1, o2) -> o1 + o2);

        //test if reduce ok
        logger.info("channel views by key: {}", channelViewsByKey.collect());

    }

}
