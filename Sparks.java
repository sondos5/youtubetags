/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Sondos
 */
public class Sparks {
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args) throws IOException {// CREATE SPARK CONTEXT
        Logger.getLogger ("org").setLevel (Level.ERROR);
        SparkConf conf= new SparkConf().setAppName("tagsCounts").setMaster("local[3]");
        JavaSparkContext sparkContext= new JavaSparkContext(conf);// LOAD DATASETS
        JavaRDD<String> videos = sparkContext.textFile("src/main/resources/data/USvideos.csv");
        // TRANSFORMATIONS
        JavaRDD<String> tags = videos
                .map (YoutubeTagsCount::extractTags)
                .filter (StringUtils::isNotBlank);
JavaRDD<String> words = tags.flatMap(title -> Arrays.asList(title.toLowerCase()
        .trim ().split ("\\| ")).iterator ());
    //COUNTING
            Map<String, Long> TagsCounts= words.countByValue();
            List<Map.Entry> sorted = TagsCounts.entrySet().stream ().sorted (Map.Entry.comparingByValue())
                    .collect (Collectors.toList());
            // DISPLAY
            for (Map.Entry<String, Long> entry : sorted) {System.out.println(entry.getKey() + " : " + entry.getValue());
            }}
    public static String extractTitle(String videoLine) {
        return videoLine.split(COMMA_DELIMITER)[6];}
}
        
    
 
}
