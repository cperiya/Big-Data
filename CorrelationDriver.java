/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author HP PC
 */
public class CorrelationDriver {
     public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tag-Ratings Correlation Join");

        job.setJarByClass(CorrelationDriver.class);

        job.setMapperClass(CorrelationMapper.class);
        job.setReducerClass(CorrelationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(args[0])); // ratings + glmer folder
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;

        System.out.println("Tag-Ratings Correlation Execution Time: " + seconds + " seconds");

        System.exit(success ? 0 : 1);
    }
}