/* Vikram Menon
 * UFID: 31935985
 * 
 * Count frequency of words in a set of text files.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;

import java.util.*;
import java.io.IOException;

public class WordCount {

   public static void main(String[] args) throws Exception {
      JobConf jobConfig = new JobConf(WordCount.class);
      
      // Configure the job to generate word counts
      FileInputFormat.setInputPaths(jobConfig, new Path(args[0]));
      FileOutputFormat.setOutputPath(jobConfig, new Path(args[1]));
      jobConfig.setJobName("WordCount");
      jobConfig.setMapperClass(Map.class);
      jobConfig.setCombinerClass(Reduce.class);
      jobConfig.setReducerClass(Reduce.class);
      jobConfig.setInputFormat(TextInputFormat.class);
      jobConfig.setOutputFormat(TextOutputFormat.class);
      jobConfig.setOutputKeyClass(Text.class);
      jobConfig.setOutputValueClass(IntWritable.class);

      JobClient.runJob(jobConfig);
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

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
      
      private Text wordText = new Text();
      
      private final static IntWritable oneIntWritable = new IntWritable(1);
      
      private final static String double_apostrophe_static = "''";
      
      private final static String single_apostrophe_static = "'";

      // Method called by Mapper
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
                  throws IOException {
         StringTokenizer lineProcessTokens = new StringTokenizer(value.toString());
         
         while (lineProcessTokens.hasMoreTokens()) {
            
            // Process the next token in the line
            String currentStr = lineProcessTokens.nextToken();
            
            if (currentStr.startsWith(double_apostrophe_static)) {
               // Ignore apostrophes in our body of text
               currentStr = currentStr.substring(2);
            } else if (currentStr.startsWith(single_apostrophe_static))
               currentStr = currentStr.substring(1);
            }
            
            wordText.set(currentStr);
            outputCollector.collect(wordText, oneIntWritable);
         }
      }
   }
}
