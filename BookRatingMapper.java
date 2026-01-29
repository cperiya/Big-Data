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
 *Book Rating - average user rating per book and top-rated books
 * 
 */
public class BookRatingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private JSONParser parser = new JSONParser();
    private Text bookId = new Text();
    private IntWritable ratingWritable = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        try {
            JSONObject obj = (JSONObject) parser.parse(value.toString());
            String itemId = obj.get("item_id").toString();
            int rating = Integer.parseInt(obj.get("rating").toString());
            bookId.set(itemId);
            ratingWritable.set(rating);
            context.write(bookId, ratingWritable);
        } catch (Exception ex) {}
    }

    
}
