/* Vikram Menon
 * UFID 31935985
 * vikmenon@ufl.edu
 * 
 * Cloud Computing programming assignment - 2
 */

package com.vikram.cloud.assignments.assgn2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vikram
 * 
 * Set initial values of previous/current pagerank to the value: BaseRank
 */
class InitializePageRank {
	
	public static long main(Job initializePageRankJob) throws Exception {
		long startTimeMillis = System.currentTimeMillis();
		initializePageRankJob.waitForCompletion(true);
		return (System.currentTimeMillis() - startTimeMillis);
	}
	
	// Reducer class for InitializePageRank
	// NOTE: Reducer does nothing here
	public static class InitializePageRankReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	// Mapper class for InitializePageRank
	public static class InitializePageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text identifier = new Text();
		
		// To place the newly set pageranks and adjacent nodes in the output as text
		private Text pageRanksAndAdjacentNodes = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String BaseRank = "1.0", EMPTY_STRING = "";
			String valueStr = value.toString();

			if (! EMPTY_STRING.equals(valueStr)) {
				// Tokenize by spaces/tabs
				StringTokenizer tokenizer = new StringTokenizer(valueStr);
				
				// Write to output with initial values of previous/current pagerank as BaseRank
				// NOTE: First token is the node identifier
				identifier.set(tokenizer.nextToken());
				pageRanksAndAdjacentNodes.set(
						" [New:" + BaseRank 
						+ "] [Old:" + BaseRank + "] " 
						+ getRemainingString(tokenizer)
					);
				context.write(identifier, pageRanksAndAdjacentNodes);
			}
		}

		private String getRemainingString(StringTokenizer tokenizer) {
			String string = "";
			while (tokenizer.hasMoreTokens())
				string += tokenizer.nextToken() + " ";
			return string;
		}
	}
}
