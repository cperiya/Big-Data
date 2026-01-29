/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


/**
 * Mapper for TagCount 
 * 
 */
public class TagCountMapper  extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final JSONParser parser = new JSONParser();

    private final IntWritable itemIdWritable = new IntWritable();
    private final Text tagCountValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONObject obj = (JSONObject) parser.parse(value.toString().trim());

            // JSON.simple always returns numbers as Long
            long itemIdL = (long) obj.get("item_id");
            long tagIdL  = (long) obj.get("tag_id");
            long numL    = (long) obj.get("num");

            int itemId = (int) itemIdL;
            int tagId  = (int) tagIdL;
            int num    = (int) numL;

            itemIdWritable.set(itemId);

            // reducer expects "tagId,num"
            tagCountValue.set(tagId + "," + num);

            context.write(itemIdWritable, tagCountValue);

        } catch (Exception e) {
            
        }
    }
}