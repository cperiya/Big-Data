/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author HP PC
 */
public class TagScoreDriver {
public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Average Tag Relevance <input> <output>");
            System.exit(1);
        }
        // Measure execution time
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Tag Relevance");
        job.setJarByClass(TagScoreDriver.class);

        job.setMapperClass(TagScoreMapper.class);
        job.setReducerClass(TagScoreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long totalTimeMs = (endTime - startTime);

        System.out.println("Average Tag Relevance Execution Time: " + totalTimeMs + " ms (" +
                           (totalTimeMs / 1000.0) + " seconds)");

        System.exit(success ? 0 : 1);
    }
}