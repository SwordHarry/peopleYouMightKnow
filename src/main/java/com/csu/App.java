package com.csu;

import com.csu.domain.FriendCountWritable;
import com.csu.domain.FriendMap;
import com.csu.domain.FriendReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
    public static void main( String[] args ) {

        Configuration conf = new Configuration();

        Job job = null;
        try {
            job = new Job(conf, "FriendRecommendation");
            job.setJarByClass(App.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(FriendCountWritable.class);

            job.setMapperClass(FriendMap.class);
            job.setReducerClass(FriendReduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileSystem outFs = new Path(args[1]).getFileSystem(conf);
            outFs.delete(new Path(args[1]), true);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
