/* Vikram Menon
 * UFID 31935985
 * vikmenon@ufl.edu
 * 
 * Cloud Computing programming assignment - 2
 */

package com.vikram.cloud.assignments.assgn2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * @Author : Vikram
 * 
 * This Class scans the graph and generates statistics such as number of nodes and edges.
 */
public class GraphNodesTraversal {
	public static long main(Job graphNodeTraversalJob) throws Exception {
		long startTimeMillis = System.currentTimeMillis();
		graphNodeTraversalJob.waitForCompletion(true);
		return (System.currentTimeMillis() - startTimeMillis);
	}

	// Mapper class for GraphNodesTraversal
	public static class GraphNodesTraversalMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		// Process each node to find edges etc.
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// We process individual nodes in the mapper and then generate aggregate statistics in the Reducer
			String EMPTY_STRING = "", INTERMEDIATE_KEY = "COMMON_REDUCER_NODE", 
					DELIMITER = " ", valueStr = value.toString();
			
			// Send out the degree of this node
			if (! EMPTY_STRING.equals(valueStr))
				context.write(new Text(INTERMEDIATE_KEY), new FloatWritable(valueStr.split(DELIMITER).length - 1));
		}
	}

	// Reducer class for GraphNodesTraversal
	public static class GraphNodesTraversalReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float totalNodeCount = -1, outDegreeSum = 0, leastOutDegree = Float.MAX_VALUE, highestOutDegree = -1;
			
			for (FloatWritable nodeDegree : values) {
				totalNodeCount++;
				
				float currentNodeOutDegree = nodeDegree.get();
				
				// Update the lowest degree
				if (leastOutDegree > currentNodeOutDegree)
					leastOutDegree = currentNodeOutDegree;
				
				// Update the highest degree
				if (highestOutDegree < currentNodeOutDegree)
					highestOutDegree = currentNodeOutDegree;
				
				// Update the outDegree sum
				outDegreeSum += currentNodeOutDegree;
			}
			
			emitStatisticsForGraph(context, outDegreeSum, leastOutDegree,
					highestOutDegree, totalNodeCount);
		}
		
		// Write out the final result
		private void emitStatisticsForGraph(Context context, float outDegreeSum,
				float lowestOutDegree, float highestOutDegree, float totalNodeCount)
						throws IOException, InterruptedException {
			// Emit the output statistics
			context.write(new Text("Number of edges in graph: "), 
					new FloatWritable(outDegreeSum));
			context.write(new Text("Number of nodes in graph: "), 
					new FloatWritable(totalNodeCount));
			context.write(new Text("Minimum out-degree at a node: "), 
					new FloatWritable(lowestOutDegree));
			context.write(new Text("Maximum out-degree at a node: "), 
					new FloatWritable(highestOutDegree));
			context.write(new Text("Average out-degree of node(s): "), new FloatWritable((float) outDegreeSum / totalNodeCount));
		}
	}
}
