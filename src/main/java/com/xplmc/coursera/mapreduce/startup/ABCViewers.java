package com.xplmc.coursera.mapreduce.startup;

import org.apache.commons.lang.math.NumberUtils;
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

/**
 * assignment for Map/Reduce
 */
public class ABCViewers {

    public static final Logger logger = LoggerFactory.getLogger(ABCViewers.class);

    public static class ABCViewersMapper
            extends Mapper<Object, Text, Text, Text> {


        /**
         * 两种类型数据：
         * Almost_News, ABC
         * Almost_News, 25
         */
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] keyValue = value.toString().split(",");
            if (keyValue.length == 2) {
                String tv = keyValue[0].trim();
                String channelOrViewers = keyValue[1].trim();
                if (NumberUtils.isDigits(channelOrViewers) || "ABC".equals(channelOrViewers)) {
                    context.write(new Text(tv.trim()), new Text(channelOrViewers.trim()));
                }
            }

        }
    }

    /**
     * 输出ABC各个节目收看人数
     */
    public static class ABCViewersReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            String tv = word.toString().trim();
            String channel = null;
            int counts = 0;
            for (Text value : values) {
                String valueStr = value.toString();
                if (NumberUtils.isDigits(valueStr)) {
                    counts += Integer.parseInt(valueStr);
                } else {
                    channel = valueStr;
                }
            }
            if (channel != null && counts > 0) {
                context.write(new Text(tv), new Text(String.valueOf(counts)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "abc viewers");
        job.setJarByClass(ABCViewers.class);
        job.setMapperClass(ABCViewersMapper.class);
//        job.setCombinerClass(ABCViewersReducer.class);
        job.setReducerClass(ABCViewersReducer.class);
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