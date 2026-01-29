/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author HP PC
 */
public class TagScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text tagKey = new Text();
    private DoubleWritable scoreValue = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Skip header line
        String line = value.toString();
        if (line.startsWith("tag,item_id,score")) {
            return;
        }

        String[] parts = line.split(",");
        if (parts.length != 3) {
            return; // ignore bad rows
        }

        try {
            String tag = parts[0].trim();
            double score = Double.parseDouble(parts[2].trim());

            tagKey.set(tag);
            scoreValue.set(score);

            context.write(tagKey, scoreValue);

        } catch (Exception e) {
            // skip malformed row
        }
    }
}
