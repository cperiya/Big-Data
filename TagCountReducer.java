/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
/**
 * Reducer for Tag Count
 * 
 */
public class TagCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private int TOP_N = 5;

    @Override
    protected void setup(Context context) {
        TOP_N = context.getConfiguration().getInt("top.n", 5);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<int[]> tags = new ArrayList<>();

        for (Text t : values) {
            String[] p = t.toString().split(",");
            if (p.length == 2) {
                try {
                    int tagId = Integer.parseInt(p[0].trim());
                    int num = Integer.parseInt(p[1].trim());
                    tags.add(new int[]{tagId, num});
                } catch (Exception ignore) {}
            }
        }

        // Sort by count DESC
        tags.sort((a, b) -> Integer.compare(b[1], a[1]));

        
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(TOP_N, tags.size());

        for (int i = 0; i < limit; i++) {
            sb.append("(")
              .append(tags.get(i)[0])
              .append(",")
              .append(tags.get(i)[1])
              .append(")");

            if (i < limit - 1) sb.append(", ");
        }

        context.write(key, new Text(sb.toString()));
    }
}