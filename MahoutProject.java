/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package edu.neu.mahoutproject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.json.JSONObject;
/**
 *
 * @author HP PC
 */
public class MahoutProject {

    public static void main(String[] args) throws IOException, TasteException {
        
        //Step1: read json file and convert it to csv
        String jsonFile = "D:/NEU/Sem4/INFO 7250 -Big Data/final project/book-genome/book_dataset/raw/ratings.json";

        String csvFile  = "ratings.csv";   

        BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));

        // Loop through JSON lines
        try (BufferedReader br = new BufferedReader(new FileReader(jsonFile))) {
            String line;
            while ((line = br.readLine()) != null) {

                // Convert single quotes to double quotes
                String fixed = line.replace('\'', '"');

                JSONObject obj = new JSONObject(fixed);

                int user   = obj.getInt("user_id");
                int item   = obj.getInt("item_id");
                int rating = obj.getInt("rating");

                writer.write(user + "," + item + "," + rating + "\n");
            }
        }

        writer.close();
        System.out.println("CSV created: " + csvFile);
        
        //Step2. Create a data model
        
        DataModel model = new FileDataModel(new File("ratings.csv"));
        
        //Step3. Find item similarity
        ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);
        //ItemSimilarity similarity = new UncenteredCosineSimilarity(model);

        
         // Step4: Create recommender engine
        GenericItemBasedRecommender recommender =
                new GenericItemBasedRecommender(model, similarity);
        
        // Step 5: Build a Recommender to Recommend top 5 items to user 1
        List<RecommendedItem> recommendations = recommender.recommend(0, 10,false);
          // Display results
        for (RecommendedItem recItem : recommendations) {
            System.out.println("Item: " + recItem.getItemID() +
                               " Score: " + recItem.getValue());
            
        }

    }
}
