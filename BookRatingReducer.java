/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author HP PC
 */
public class BookRatingReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable avgWritable = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0, count = 0;

        for (IntWritable v : values) {
            sum += v.get();
            count++;
        }

        double avg = (double) sum / count;
        avgWritable.set(avg);
        context.write(key, avgWritable);
    }
}