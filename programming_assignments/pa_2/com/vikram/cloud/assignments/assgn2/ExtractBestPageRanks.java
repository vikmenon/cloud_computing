/* Vikram Menon
 * UFID 31935985
 * vikmenon@ufl.edu
 * 
 * Cloud Computing programming assignment - 2
 */

package com.vikram.cloud.assignments.assgn2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

class ExtractBestPageRanks {
	private static String DELIMITER="~";
	private static Integer NUM_TOP_PAGERANKS = 10;
	
	public static long main(Job extractBestPageRanks) throws Exception {
		long startTimeMillis = System.currentTimeMillis();
		extractBestPageRanks.waitForCompletion(true);
		return (System.currentTimeMillis() - startTimeMillis);
	}

	// Mapper class for ExtractBestPageRanks
	// Passes the converged pagerank for each page to the Reducer
	public static class ExtractBestPageRanksMapper extends Mapper<LongWritable, Text, Text, Text> {
		// We pass all the map outputs to a single reducer for fetching the best NUM_TOP_PAGERANKS pageranked pages.
		private String INTERMEDIATE_KEY = "COMMON_REDUCER";
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueStr = value.toString(), EMPTY_STRING = "";
			if (! EMPTY_STRING.equals(valueStr)) {
				StringTokenizer tokens = new StringTokenizer(valueStr);
				String currentNode = tokens.nextToken();
				
				// The next token is the new pagerank;
				// Write out NodeID => pagerank value
				String pageRank = CheckConvergence.CheckConvergenceMapper.cleanupFloatStr(tokens.nextToken());
				context.write(new Text(INTERMEDIATE_KEY), 
						new Text(currentNode + DELIMITER + pageRank)
					);
			}
		}
	}
	
	// Reducer class for ExtractBestPageRanks
	// Calculates the top NUM_TOP_PAGERANKS pageranks from the complete list
	public static class ExtractBestPageRanksReducer extends Reducer<Text, Text, Text, FloatWritable> {
		private void emitTopPageRanks(Context context, ArrayList<NodeRankStruct> bestNodesAndRanks)
				throws IOException, InterruptedException {
			// Emit the computed pageranks in decreasing order
			System.out.println("Number of page ranks to be "
					+ "emitted is now :: " + bestNodesAndRanks.size());
			Float highestRemainingValue = Float.MAX_VALUE;
			for (int i = 0; i < NUM_TOP_PAGERANKS; i++) {
				highestRemainingValue = Float.MIN_VALUE;
				
				// Find the highest pagerank in the remaining list
				for (NodeRankStruct nodeRankPair : bestNodesAndRanks)
					if (nodeRankPair.pagerank > highestRemainingValue)
						highestRemainingValue = nodeRankPair.pagerank;
				
				// Extract that pair
				NodeRankStruct currentPair = bestNodesAndRanks
												.remove(
														bestNodesAndRanks.indexOf(highestRemainingValue)
													);
				
				// Emit that pair
				context.write(new Text(currentPair.nodeID), new FloatWritable(currentPair.pagerank));
			}
		}
	
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			float currLowestPageRankInList = Float.MAX_VALUE;
			int currentMinimumValueIndexInList = 0;
			ArrayList<NodeRankStruct> bestNodesAndRanks = new ArrayList<NodeRankStruct>();
			
			for (Text dataToReduce : values) {
				// Each datatToReduce string is of the form "nodeID DELIMITER latest pagerank"
				String currentNodeIdentifier = dataToReduce.toString().split(DELIMITER)[0];
				float currentNodePageRank = Float.parseFloat(dataToReduce.toString().split(DELIMITER)[1]);
				
				if (bestNodesAndRanks.size() < NUM_TOP_PAGERANKS) {
					
					// We can add an entry to the list immediately
					bestNodesAndRanks.add(new NodeRankStruct(currentNodeIdentifier, currentNodePageRank));
					
					// Update indices
					if (currentNodePageRank < currLowestPageRankInList) {
						currentMinimumValueIndexInList = bestNodesAndRanks.size() - 1;
						currLowestPageRankInList = currentNodePageRank;
					}
				}
				else {
					
					// Replace an entry from the list of top NUM_TOP_PAGERANKS
					
					System.out.println(
							"Size exceeds 10, so we must remove an element from the list of best nodes. "
							+ bestNodesAndRanks.size());

					if (currentNodePageRank > currLowestPageRankInList) {
						// Replace the last entry in the list
						bestNodesAndRanks.remove(currentMinimumValueIndexInList);
						
						// Add the new entry in the list
						bestNodesAndRanks.add(new NodeRankStruct(currentNodeIdentifier, currentNodePageRank));
						
						// Find the new lowest entry in the list of top NUM_TOP_PAGERANKS
						currLowestPageRankInList = Float.MAX_VALUE;
						for (int iter = 0;
							iter < bestNodesAndRanks.size();
							iter++) {
							if (bestNodesAndRanks.get(iter)
									.pagerank < currLowestPageRankInList) {
								currentMinimumValueIndexInList = iter;
								currLowestPageRankInList = bestNodesAndRanks.get(iter).pagerank;
							}
						}
					}
				}
			}
			
			// Write out the result
			emitTopPageRanks(context, bestNodesAndRanks);
		}
	}

	// Used to output the final value in mapreduce
	private static class NodeRankStruct {
		String nodeID;
		Float pagerank;
		
		public NodeRankStruct(String nodeID, Float pagerank) {
			this.nodeID = nodeID;
			this.pagerank = pagerank;
		}
	}
}