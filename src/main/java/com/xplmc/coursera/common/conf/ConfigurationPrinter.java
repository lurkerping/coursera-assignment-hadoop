package com.xplmc.coursera.common.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * print hadoop cluster configurations
 */
public class ConfigurationPrinter extends Configured implements Tool {

    public static final Logger logger = LoggerFactory.getLogger(ConfigurationPrinter.class);

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
//            logger.info("{}->{}", entry.getKey(), entry.getValue());
            System.out.println((String.format("%s->%s", entry.getKey(), entry.getValue())));
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }


}
