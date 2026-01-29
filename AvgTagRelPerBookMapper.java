/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author HP PC
 */
public class AvgTagRelPerBookMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private IntWritable itemIdOut = new IntWritable();
    private DoubleWritable scoreOut = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("tag")) return; // skip header

        // CSV format: tag,item_id,score
        String[] parts = line.split(",");

        if (parts.length != 3) return;

        try {
            int item_id = Integer.parseInt(parts[1].trim());
            double score = Double.parseDouble(parts[2].trim());

            itemIdOut.set(item_id);
            scoreOut.set(score);

            context.write(itemIdOut, scoreOut);

        } catch (Exception e) {
            // skip malformed lines
        }
    }
    
}
