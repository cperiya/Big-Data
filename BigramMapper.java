/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author HP PC
 */
public class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private JSONParser parser = new JSONParser();
    private IntWritable one = new IntWritable(1);
    private Text bigramText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        try {
            JSONObject obj = (JSONObject) parser.parse(value.toString());
            String review = obj.get("txt").toString().toLowerCase();
            String[] words = review.split("\\W+");
            for (int i = 0; i < words.length - 1; i++) {
                String w1 = words[i];
                String w2 = words[i + 1];
                if (w1.length() > 0 && w2.length() > 0) {
                    bigramText.set(w1 + " " + w2);
                    context.write(bigramText, one);
                }
            }
        } catch (Exception e) {}
    }
   
}
