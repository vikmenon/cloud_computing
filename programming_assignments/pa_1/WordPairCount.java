/* Vikram Menon
 * UFID: 31935985
 * 
 * Count frequency of word-pairs in a set of text files.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class WordPairCount {
   
   public static void main(String[] args) throws Exception {
      JobConf jobConfig = new JobConf(WordPairCount.class);
      
      // Configure the job to generate word-pair counts
      FileInputFormat.setInputPaths(jobConfig, new Path(args[0]));
      FileOutputFormat.setOutputPath(jobConfig, new Path(args[1]));
      jobConfig.setJobName("WordPairCount");
      jobConfig.setMapperClass(Map.class);
      jobConfig.setCombinerClass(Reduce.class);
      jobConfig.setReducerClass(Reduce.class);
      jobConfig.setInputFormat(TextInputFormat.class);
      jobConfig.setOutputFormat(TextOutputFormat.class);
      jobConfig.setOutputKeyClass(Text.class);
      jobConfig.setOutputValueClass(IntWritable.class);

      JobClient.runJob(jobConfig);
   }
   
   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
      
      private Text wordText = new Text();
      
      private final static IntWritable oneIntWritable = new IntWritable(1);
      
      private final static String double_apostrophe_static = "''";
      
      private final static String single_apostrophe_static = "'";
      
      // Method called by Mapper
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
            throws IOException {
         StringTokenizer lineProcessTokens = new StringTokenizer(value.toString());
         
         // Used to track pairs
         String lastWord = "";
         
         while (lineProcessTokens.hasMoreTokens()) {
            String currentToken = lineProcessTokens.nextToken();
            
            if (currentToken.startsWith(double_apostrophe_static) {
               currentToken = currentToken.substring(2);
            } else if (currentToken.startsWith(single_apostrophe_static)) {
               currentToken = currentToken.substring(1);
            }
            
            // Update lastWord to the current token if it is not set i.e. first iteration of the loop
            if (lastWord == "") {
               lastWord = currentToken;
               continue;
            }
            
            // If lastWord is set then add this word pair to the output
            String wordPair = lastWord + " " + currentToken;
            wordText.set(wordPair);
            outputCollector.collect(wordText, oneIntWritable);
            
            lastWord = currentToken;
         }
      }
   }
   
   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
      
      // Method called by Reducer
      public void reduce(Text key, Iterator<IntWritable> valueIter, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
            throws IOException {
         // Calculate the sum of values here
         int total = 0;
         
         while (valueIter.hasNext())
            total += valueIter.next().get();
         
         outputCollector.collect(key, new IntWritable(total));
      }
      
   }
   
}
