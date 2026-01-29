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
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text wordOut = new Text();
    private final JSONParser parser = new JSONParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {

        try {
            JSONObject obj = (JSONObject) parser.parse(value.toString());

            if (!obj.containsKey("txt")) return;

            String review = obj.get("txt").toString().toLowerCase();

            // Clean & tokenize
            String[] words = review
                    .replaceAll("[^a-z0-9 ]", " ")
                    .replaceAll("\\s+", " ")
                    .trim()
                    .split(" ");

            for (String w : words) {
                if (!w.isEmpty()) {
                    wordOut.set(w);
                    context.write(wordOut, ONE);
                }
            }

        } catch (Exception e) {
            // Skip bad JSON lines
        }
    }
}