package edu.capsl.fdp.training;

/*	
 * Copyright (C) 2014 Computer Architecture and Parallel Systems Laboratory (CAPSL)	
 *
 * Original author: Sergio Pino	
 * E-Mail: sergiop@udel.edu
 *
 * License
 * 	
 * Redistribution of this code is allowed only after an explicit permission is
 * given by the original author or CAPSL and this license should be included in
 * all files, either existing or new ones. Modifying the code is allowed, but
 * the original author and/or CAPSL must be notified about these modifications.
 * The original author and/or CAPSL is also allowed to use these modifications
 * and publicly report results that include them. Appropriate acknowledgments
 * to everyone who made the modifications will be added in this case.
 *
 * Warranty	
 *
 * THIS CODE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING, WITHOUT LIMITATION, WARRANTIES THAT
 * THE COVERED CODE IS FREE OF DEFECTS, MERCHANTABLE, FIT FOR A PARTICULAR
 * PURPOSE OR NON-INFRINGING. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE
 * OF THE COVERED CODE IS WITH YOU. SHOULD ANY COVERED CODE PROVE DEFECTIVE IN
 * ANY RESPECT, YOU (NOT THE INITIAL DEVELOPER OR ANY OTHER CONTRIBUTOR) ASSUME
 * THE COST OF ANY NECESSARY SERVICING, REPAIR OR CORRECTION. THIS DISCLAIMER
 * OF WARRANTY CONSTITUTES AN ESSENTIAL PART OF THIS LICENSE. NO USE OF ANY
 * COVERED CODE IS AUTHORIZED HEREUNDER EXCEPT UNDER THIS DISCLAIMER.
 */

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MCTraining {

	
	public static class TrSeqBuilderMap extends Mapper<LongWritable, Text, CompositeKey, Text> {
		
		private final Log LOG = LogFactory.getLog(TrSeqBuilderMap.class);
		private CompositeKey outkey = new CompositeKey();
		private Text trType = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
				throws IOException, InterruptedException {

			// each record has the format cID, tID, tType
			String[] tokens = value.toString().split(",");
			
			if (tokens.length == 3) {
				String cID = tokens[0].trim();
				long tID = Long.parseLong(tokens[1].trim());
				String tType = tokens[2].trim();
				
				// creating the composite key
				outkey.setcID(cID);
				outkey.settID(tID);
				
				// setting the transaction type
				trType.set(tType);
				
				LOG.info("emitting " + outkey + ":" + trType);
				
				context.write(outkey, trType);
				
			} else {
				LOG.warn("The tokens' length is not 3, intead is " + tokens.length);
			}
		}
	}
	
	public static class TrSeqBuilderRed extends Reducer<CompositeKey, Text, Text, Text> {
		@Override
		protected void reduce(CompositeKey key, Iterable<Text> values,
				Reducer<CompositeKey, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			System.out.println("Key: " + key.getcID());
			
			for (Text val : values)
				System.out.print(val.toString() + ", ");
			
			System.out.println();
		}
	}
	
	
	
	public static Job getSequenceBuilderJob(String[] args) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
	    
	    Job job = new Job(conf);
	    job.setJobName("SequenceBuilder");
		job.setJarByClass(MCTraining.class);
		
	    // mapper configuration
	    job.setMapperClass(TrSeqBuilderMap.class);
	    job.setMapOutputKeyClass(CompositeKey.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    // intermediate
	    job.setPartitionerClass(CompositeKey.CompositeKeyPartitioner.class);
	    job.setGroupingComparatorClass(CompositeKey.CompositeKeyGroupComparator.class);
	    job.setSortComparatorClass(CompositeKey.CompositeKeySortComparator.class);
	    
	    // reducer configuration
	    job.setReducerClass(TrSeqBuilderRed.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    FileOutputFormat.setOutputPath(job,
	      new Path(args[1]));
	    
	    return job;
	}
	
	public static void main(String[] args) throws IllegalArgumentException, Exception {
		
		
		Job seq_builder = getSequenceBuilderJob(args);
		
	    System.exit(seq_builder.waitForCompletion(true) ? 0 : 1);
		
	}
}
