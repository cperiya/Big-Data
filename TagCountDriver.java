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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for TagCount
 * 
 */
public class TagCountDriver {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Error: TagCountDriver <input> <output><N>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        System.out.println("Top-N Tag Count Job Started at: " + startTime);

        Configuration conf = new Configuration();
        conf.setInt("top.n", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Top-N Tags Per Book");
        job.setJarByClass(TagCountDriver.class);

        job.setMapperClass(TagCountMapper.class);
        job.setReducerClass(TagCountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long elapsed = endTime - startTime;

        System.out.println("Top-N Tag Count Job Finished at: " + endTime);
        System.out.println("Total Execution Time: " + elapsed + " ms (" + (elapsed / 1000.0) + " seconds) ===");

        System.exit(success ? 0 : 1);
    }
}