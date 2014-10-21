/* Vikram Menon
 * UFID 31935985
 * vikmenon@ufl.edu
 * 
 * Cloud Computing programming assignment - 2
 */

package com.vikram.cloud.assignments.assgn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.vikram.cloud.assignments.assgn2.CheckConvergence.CheckConvergenceMapper;
import com.vikram.cloud.assignments.assgn2.CheckConvergence.CheckConvergenceReducer;
import com.vikram.cloud.assignments.assgn2.ExtractBestPageRanks.ExtractBestPageRanksMapper;
import com.vikram.cloud.assignments.assgn2.ExtractBestPageRanks.ExtractBestPageRanksReducer;
import com.vikram.cloud.assignments.assgn2.GraphNodesTraversal.GraphNodesTraversalMapper;
import com.vikram.cloud.assignments.assgn2.GraphNodesTraversal.GraphNodesTraversalReducer;
import com.vikram.cloud.assignments.assgn2.InitializePageRank.InitializePageRankMapper;
import com.vikram.cloud.assignments.assgn2.InitializePageRank.InitializePageRankReducer;

/**
 * @author Vikram
 * 
 * This class generates graph statistics, computes the pagerank over a number of iterations of the pagerank algorithm until 
 * the change between iterations is below threshold (convergence) and returns the resultant top ten list.
 */

public class PageRankComputation {
	private static int MAX_ITERATIONS = 100;

	public static class ResultStructure {
		long nodesLeft;
		long timeTaken;
		
		ResultStructure(long nodesLeft, long timeTaken) {
			this.nodesLeft = nodesLeft;
			this.timeTaken = timeTaken;
		}
	}
	
	@SuppressWarnings("unused")
	private static class PostIteration {
		int iterations;
		long timeTaken;
		
		public PostIteration(int iterations, long timeTaken) {
			this.iterations = iterations;
			this.timeTaken = timeTaken;
		}
	}
	
	// The entire page rank algorithm
	public static void main(String[] args) throws Exception {
		String inputFileArg = args[0];		// Input file defines the graph
		String outputFolderArg = args[1]; 	// Write mapreduce output here
		
		long startTimeMillis = System.currentTimeMillis();

		// Page Rank Algorithm steps
		RunGraphNodesTraversalJob(inputFileArg, outputFolderArg);
		RunInitializePageRankJob(inputFileArg, outputFolderArg);
		RunExtractTopTenJob(
				RunUntilConvergence(outputFolderArg),		// This function runs mapreduce jobs in a loop
				outputFolderArg
			);
		
		// Calculate total time taken to run this program.
		System.out.println("NOTE: Total runtime of the PageRank program was: \t" 
				+ ((float) (System.currentTimeMillis() - startTimeMillis))/1000.0 
				+ " seconds.");
		System.out.println("--- Terminated program. ---");
	}
	
	/*
	 * Configure, create and return a mapreduce job.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static Job CreateJob(String jobName, Class outputValueClass, Class jarByClass, 
		Class mapperClass, Class reducerClass, String inputPath, String outputPath)
			throws IOException {
		// Parameters
		Job newJob = new Job(new Configuration(), jobName);
		newJob.setJarByClass(jarByClass);
		newJob.setOutputValueClass(outputValueClass);
		newJob.setMapperClass(mapperClass);
		newJob.setReducerClass(reducerClass);
		
		// I/O path
		FileInputFormat.addInputPath(newJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(newJob, new Path(outputPath));
		
		// Fixed setup
		newJob.setOutputKeyClass(Text.class);
		newJob.setInputFormatClass(TextInputFormat.class);
		newJob.setOutputFormatClass(TextOutputFormat.class);
		return newJob;
	}
	
	/* 
	 * Extract graph properties by processing each node in the map phase, and generating statistics in the reduce phase.
	 */
	public static long RunGraphNodesTraversalJob(String inputFile, String outputFolder)
			throws IOException, Exception {
		long timeTaken = GraphNodesTraversal.main(
				CreateJob("AnalyzeGraph", 
						FloatWritable.class, 
						GraphNodesTraversal.class, 
						GraphNodesTraversalMapper.class, 
						GraphNodesTraversalReducer.class, 
						inputFile, 
						outputFolder + "/GraphPropertiesSummary")
					);
		System.out.println(
				"NOTE: Graph traversal was completed in: "
				+ ((float) timeTaken) / 1000.0 + " seconds.");
		return timeTaken;
	}
	
	/* 
	 * Initialize page rank for the (#nodes * #nodes) matrix setting 1.0 as the previous & current values of pagerank.
	 */
	public static long RunInitializePageRankJob(String inputFile, String outputFolder)
			throws IOException, Exception {
		long timeTaken = InitializePageRank.main(
				CreateJob("InitializePageRankAlgorithm", 
						Text.class, 
						InitializePageRank.class, 
						InitializePageRankMapper.class, 
						InitializePageRankReducer.class, 
						inputFile, 
						outputFolder + "/iteration_0")
					);
		System.out.println("NOTE: Initializing pageranks to the value 1.0 took "
						+ ((float) timeTaken) / 1000.0 + " seconds.");
		return timeTaken;
	}
	
	/* 
	 * Iterate over the (#nodes * #nodes) matrix at most MAX_ITERATIONS times to converge on 
	 * the pagerank using the formula in the PageRank paper. Convergence occurs when 
	 * differenceInThisIteration < THRESHOLD as configured in CheckConvergence class.
	 */
	private static PostIteration RunUntilConvergence(String outputFolderArg)
			throws IOException, Exception {
		boolean found = false;
		int i = 1;	// Since pagerank is already initialized
		long totalTime = 0;
		ResultStructure passResult = null;
		
		System.out.println("NOTE: The page rank algorithm now iterates over the graph the until convergence is reached.");
		for (i = 1; ! found && i < MAX_ITERATIONS; i++) {
			passResult = CheckConvergence.main(
							CreateJob("CheckConvergence", 
									Text.class, 
									CheckConvergence.class, 
									CheckConvergenceMapper.class, 
									CheckConvergenceReducer.class, 
									outputFolderArg + "/iteration_" + (i - 1) + "/", 
									outputFolderArg + "/iteration_" + i)
						);
			System.out.println("\nNOTE: Iteration #" + i + " completed in: " + "\t" + ((float) passResult.timeTaken)/1000.0 + " seconds.");
			totalTime += passResult.timeTaken;
			
			// Should the algorithm terminate here? Only if every node has converged i.e.
			if (i > 1
					&& 0 == passResult.nodesLeft) {
				System.out.println("NOTE: The page rank algorithm has reached convergence!\n");
				found = true;
			}
		}
		
		System.out.println("NOTE: Total number of iterations is: " + (i - 1));
		return new PostIteration(i, totalTime);
	}

	/* 
	 * Return the 10 top pages (best pageranks) to the user.
	 */
	private static long RunExtractTopTenJob(PostIteration postIteration, String outputFolderArg)
			throws IOException, Exception {
		System.out.println("Now extracting the best 10 page ranks from the graph...");
		long timeTaken = ExtractBestPageRanks.main(
				CreateJob("ExtractBestPageRanks", 
						Text.class, 
						ExtractBestPageRanks.class, 
						ExtractBestPageRanksMapper.class, 
						ExtractBestPageRanksReducer.class, 
						outputFolderArg + "/iteration_" + (postIteration.iterations - 1) + "/", 
						outputFolderArg + "/Top_10_Results")
				);
		System.out.println("NOTE: Time taken to extract the best 10 page ranks from the graph: " + "\t" + ((float) timeTaken)/1000.0 + " seconds.");
		return timeTaken;
	}
}
