/* Vikram Menon
 * UFID: 31935985
 * 
 * Program to use DistributedCache to count frequency of words specified by word-patterns.txt in another text.
 */

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DistributedCacheWordCount {
   private final static String aws_word_patterns_path = "s3://vikram-menon/input-pattern/word-patterns.txt";
   private final static String fg_word_patterns_path = "word-patterns.txt";

   public static void main(String[] args)
         throws Exception {
      JobConf jobConfig = new JobConf(DistributedCacheWordCount.class);
      
      // Set up the job to calculate word counts
      jobConfig.setJobName("DistCacheWC");
      jobConfig.setInputFormat(TextInputFormat.class);
      jobConfig.setOutputFormat(TextOutputFormat.class);
      jobConfig.setMapperClass(Map.class);
      jobConfig.setCombinerClass(Reduce.class);
      jobConfig.setReducerClass(Reduce.class);
      jobConfig.setOutputKeyClass(Text.class);
      jobConfig.setOutputValueClass(IntWritable.class);

      DistributedCache.addCacheFile(
            new Path(aws_word_patterns_path), jobConfig
         );
      
      FileInputFormat.setInputPaths(jobConfig, new Path(args[0]));
      FileOutputFormat.setOutputPath(jobConfig, new Path(args[1]));
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
      private final static IntWritable oneIntWritable = new IntWritable(1);
      private final static String local_file_prefix = "localfiles";
  
      public Path[] filesToCache;
      private Text wordText = new Text();
      
      // The patterns to read are fetched from word-patterns.txt
      private HashMap<String, Integer> countPatterns = new HashMap<String, Integer>();

      // Method called by Mapper
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
                  throws IOException {
         // Tokenize and process the value for this key
         StringTokenizer lineProcessTokens = new StringTokenizer(value.toString());
         System.out.println(countPatterns.toString());
         while (lineProcessTokens.hasMoreTokens())
         {
            String currentString = lineProcessTokens.nextToken();
            
            if(countPatterns.containsKey(currentString))
            {
               wordText.set(currentString);
               outputCollector.collect(wordText, oneIntWritable);
            }
         }
      }
      
      // This method pre-populates countPatterns with the words from word-patterns.txt
      public void configure(JobConf jobConfiguration) {
         try {
            filesToCache = DistributedCache.getLocalCacheFiles(jobConfiguration);
            BufferedReader bufReader = new BufferedReader(
                                    new FileReader(
                                       filesToCache[0].toString()
                                    ));
            
            String curISLine = null;
            while ((curISLine = bufReader.readLine()) != null)
            {
               StringTokenizer lineProcessTokens = new StringTokenizer(curISLine);
               while (lineProcessTokens.hasMoreTokens())
                  countPatterns.put((String) lineProcessTokens.nextToken(), 1);
            }
            bufReader.close();
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
      
    }

}
