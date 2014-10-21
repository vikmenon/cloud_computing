/* Vikram Menon
 * UFID 31935985
 * vikmenon@ufl.edu
 * 
 * Cloud Computing programming assignment - 2
 */

package com.vikram.cloud.assignments.assgn2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.vikram.cloud.assignments.assgn2.PageRankComputation.ResultStructure;

/**
 * @author Vikram
 * 
 * Check if convergence has been reached by going through all the nodes and comparing previous 
 * and current values of pagerank.
 */
public class CheckConvergence {
	
	// Suggested value from paper; this takes care of the case where a user does not follow a link but jumps to a random page
	public static float PageRankDampingFactor = 0.85f;

	/* Mapper class for CheckConvergence
	 * Here we update the pageranks for all nodes.
	 */
	public static class CheckConvergenceMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text identifier = new Text();
		
		// Convergence Threshold between iterations
		private float THRESHOLD = 0.001f;
		
		private Text partialPageRankValue = new Text();
		private Text adjacentNodesList = new Text();
		
		// Extract the pagerank from the format in the intermediate file
		public static String cleanupFloatStr(String str1) {
			System.out.println("NOTE: Cleaning up " + str1);
			Matcher match = Pattern.compile("\\[(New|Old):(\\d+(\\.\\d*)?)\\]").matcher(str1);
			try {
				match.find();
				return match.group(2);
			} catch (Exception e) {
				System.out.println("ERR: Caught an exception while matching pattern! str1: " + str1);
				return null;
			}
		}
		
		// Update the page rank and test for convergence; accordingly update ProcessData.Unconverged
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueStr = value.toString();
			String EMPTY_STRING = "", DATATYPE = "f";
			float dampingFactorValue = 1.0f - PageRankDampingFactor;

			if (! EMPTY_STRING.equals(valueStr)) {
				StringTokenizer tokenizer = new StringTokenizer(valueStr);
				
				// Three tokens are taken up for identifier, previous pagerank, current pagerank
				float nodeOutDegree = tokenizer.countTokens() - 3;
				
				// Node ID
				String currentNode = tokenizer.nextToken();
				
				// Previous pagerank value
				Float parentPageRank = Float.parseFloat(
						cleanupFloatStr(tokenizer.nextToken()));
				String rhsString = " [Old:" + parentPageRank + "] ";
				
				// Grandparent pagerank
				Float grandParentPageRank = Float.parseFloat(cleanupFloatStr(tokenizer.nextToken()));
				
				// Check if difference crossed the threshold
				if ((Math.abs(parentPageRank - grandParentPageRank) > THRESHOLD)) {
					System.out.println(currentNode);
					
					// Mark this node as converged for our tracking purposes
					context.getCounter(ProcessData.Unconverged).increment(1);
				}
				
				// Calculate the new pagerank if any links are available
				while (nodeOutDegree > 0
						&& tokenizer.hasMoreTokens()) {
					String nextAdjacentNode = tokenizer.nextToken();
					
					// Calculate next pagerank value
					String partialPageRankUpdated = Float.toString((1-dampingFactorValue) * (parentPageRank/nodeOutDegree)) + DATATYPE;

					identifier.set(nextAdjacentNode);
					partialPageRankValue.set(partialPageRankUpdated);
					
					// Write out the partial pagerank value for the adjacent node
					rhsString += nextAdjacentNode + " ";
					context.write(identifier, partialPageRankValue);
				}
				
				// Write out the list of adjacent nodes to this one
				identifier.set(currentNode);
				adjacentNodesList.set(rhsString);
				context.write(identifier, adjacentNodesList);

				// Write out the partial pagerank for this page
				partialPageRankValue.set(
						Float.toString(dampingFactorValue)
						+ DATATYPE
					);
				context.write(identifier, partialPageRankValue);
			}
		}
	}

	// Reducer class for CheckConvergence
	public static class CheckConvergenceReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> partialPageRankValues, Context context) throws IOException, InterruptedException {
			float pageRankSum = 0;
			String DATATYPE = "f", adjacentNodesList = "";
			
			// Add up all the partial pageranks for a node
			for (Text pageRankVal : partialPageRankValues) {
				String stringVal = pageRankVal.toString();
				int lastCharIndex = stringVal.length() - 1;
				
				if (DATATYPE.charAt(0) != stringVal.charAt(lastCharIndex)) {
					// Is list of adjacent nodes
					adjacentNodesList = stringVal;
				}
				else {
					// Is a partial page rank value
					pageRankSum += Float.parseFloat(
							stringVal.substring(0, lastCharIndex)
						);
				}
			}
			
			// Write out the new and old pagerank values and adjacent nodes to this one
			context.write(key, 
					new Text("[New:" + Float.toString(pageRankSum) + "] " + adjacentNodesList)
				);
		}
	}

	// Keep track of the number of pages yet to converge in the pagerank algorithm
	public static 
		enum ProcessData {
		Unconverged;
	};
	
	// Updates pageranks and returns time taken and number of nodes left to converge within accuracy of THRESHOLD.
	public static ResultStructure main(Job checkConvergenceJob) throws Exception {
		long startTimeMillis = System.currentTimeMillis();
		checkConvergenceJob.waitForCompletion(true);
		return new ResultStructure(
				checkConvergenceJob
					.getCounters()
						.findCounter(ProcessData.Unconverged).getValue(), 
				System.currentTimeMillis() - startTimeMillis
			);
	}
}
