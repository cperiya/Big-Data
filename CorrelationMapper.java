/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.neu.taggenomemr;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author HP PC
 */
public class CorrelationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable outKey = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Ratings JSON: {"item_id": 41335427, "user_id": 0, "rating": 5}
        if (line.startsWith("{")) {
            try {
                line = line.replace("{", "")
                           .replace("}", "")
                           .replace("\"", "");

                String[] parts = line.split(",");
                int itemId = Integer.parseInt(parts[0].split(":")[1].trim());
                int rating = Integer.parseInt(parts[2].split(":")[1].trim());

                outKey.set(itemId);

                context.write(outKey, new Text("R|" + rating));

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }

        // Tag Genome CSV: tag,item_id,score
        else if (line.contains(",")) {
            try {
                String[] parts = line.split(",");
                if (parts.length < 3) return;

                String tag = parts[0].trim();
                int itemId = Integer.parseInt(parts[1].trim());
                double score = Double.parseDouble(parts[2].trim());

                outKey.set(itemId);

                context.write(outKey, new Text("T|" + tag + "|" + score));

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }
    }
}