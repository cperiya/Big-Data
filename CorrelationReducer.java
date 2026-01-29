/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author HP PC
 */
public class CorrelationReducer  extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<Integer> ratings = new ArrayList<>();
        ArrayList<String> tags = new ArrayList<>();

        for (Text v : values) {
            String[] parts = v.toString().split("\\|");

            if (parts[0].equals("R")) {
                ratings.add(Integer.parseInt(parts[1]));
            } else if (parts[0].equals("T")) {
                tags.add(parts[1] + "|" + parts[2]); // tag|score
            }
        }

        // Now join: for each rating combine with each tag entry
        for (Integer r : ratings) {
            for (String t : tags) {
                String[] tagParts = t.split("\\|");

                String tag = tagParts[0];
                String score = tagParts[1];

                context.write(
                        new Text(String.valueOf(key.get())),
                        new Text(r + "\t" + tag + "\t" + score)
                );
            }
        }
    }
}