/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author HP PC
 */
public class BookRatingDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BookAvgRating <input> <output>");
            System.exit(1);
        }
        long startTime = System.currentTimeMillis();
        System.out.println("=== Average Rating per Book Job Started at: " + startTime + " ===");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Rating per Book");

        job.setJarByClass(BookRatingDriver.class);
        
        // Set Mapper and Reducer
        job.setMapperClass(BookRatingMapper.class);
        job.setReducerClass(BookRatingReducer.class);
        job.setNumReduceTasks(1);
        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input/Output paths
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        System.out.println("=== Average Rating per Book Job Finished at: " + endTime + " ===");
        System.out.println("=== Total Execution Time: " + duration + " ms (" + (duration / 1000.0) + " seconds) ===");

        System.exit(success ? 0 : 1);
    }
}