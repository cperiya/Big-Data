/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author HP PC
 */
public class TagScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable avgScore = new DoubleWritable();

    @Override
    protected void reduce(Text tag, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        long count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            avgScore.set(sum / count);
            context.write(tag, avgScore);
        }
    }
}