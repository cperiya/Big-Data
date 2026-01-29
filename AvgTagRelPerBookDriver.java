/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author HP PC
 */
public class AvgTagRelPerBookDriver {
     public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();   // Start execution timer

        if (args.length != 2) {
            System.err.println("Usage: Avg Tag Relevance per Book <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Tag Relevance Per Book");

        job.setJarByClass(AvgTagRelPerBookDriver.class);

        job.setMapperClass(AvgTagRelPerBookMapper.class);
        job.setReducerClass(AvgTagRelPerBookReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();   
        double durationSec = (endTime - startTime) / 1000.0;

        System.out.println("Avg Tag Relevance Per Book Execution Time: " + durationSec + " seconds");
        

        System.exit(success ? 0 : 1);
    }
}
