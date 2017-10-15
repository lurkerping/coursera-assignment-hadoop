package com.xplmc.coursera.mapreduce.startup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * simple join example
 */
public class SimpleJoin {

    private static final Logger logger = LoggerFactory.getLogger(SimpleJoin.class);

    public static class SimpleJoinMapper
            extends Mapper<Object, Text, Text, Text> {


        /**
         * two kinds of data formats:
         * Jan-01 able,5
         * able,991
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] keyValue = value.toString().split(",");
            if (keyValue.length == 2) {
                String[] inputKey = keyValue[0].split(" ");
                String inputValue = keyValue[1];
                if (inputKey.length >= 2) {
                    String date = inputKey[0];
                    String word = inputKey[1];
                    logger.info("------:" + word + "," + date + " " + inputValue);
                    context.write(new Text(word), new Text(date + " " + inputValue));
                } else {
                    logger.info("------:" + inputKey[0] + "," + inputValue);
                    context.write(new Text(inputKey[0]), new Text(inputValue));
                }
            }

        }
    }

    /**
     * inputs format 1: word,totalCount
     * inputs format 2: word,date count
     * outputs format: date word countByDate totalCount
     */
    public static class SimpleJoinReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Map<String, String> output = new HashMap<>();
            String totalCount = "0";
            for (Text value : values) {
                String[] valueArray = value.toString().split(" ");
                if (valueArray.length == 2) {
                    String date = valueArray[0];
                    String dateCount = valueArray[1];
                    output.put(date + " " + word.toString(), dateCount);
                } else {
                    totalCount = valueArray[0];
                }
                logger.info("++++++:" + word + "," + value.toString());
            }
            for (Map.Entry<String, String> entry : output.entrySet()) {
                logger.info("======:" + entry.getKey() + "," + entry.getValue() + " " + totalCount);
                context.write(new Text(entry.getKey()), new Text(entry.getValue() + " " + totalCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join word");
        job.setJarByClass(SimpleJoin.class);
        job.setMapperClass(SimpleJoinMapper.class);
//        job.setCombinerClass(ABCViewerReducer.class);
        job.setReducerClass(SimpleJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (args.length > 2) {
            try {
                job.setNumReduceTasks(Integer.parseInt(args[2]));
            } catch (Exception e) {
                //heihei
            }
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}