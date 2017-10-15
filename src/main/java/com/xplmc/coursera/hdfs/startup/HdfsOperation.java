package com.xplmc.coursera.hdfs.startup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * hadoop simple operation
 */
public class HdfsOperation {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    private static final String HDFS_URL = "hdfs://sandbox.hortonworks.com:8020";

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI(HDFS_URL), conf);

            try (InputStream is = fs.open(new Path(HDFS_URL + "/test/coursera/hdfs/abc.txt"))) {
                IOUtils.copyBytes(is, System.out, 8192);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void main1(String[] args) {
        try (InputStream is = new URL(HDFS_URL + "/test/coursera/hdfs/abc.txt").openStream()) {
            IOUtils.copyBytes(is, System.out, 8192);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
