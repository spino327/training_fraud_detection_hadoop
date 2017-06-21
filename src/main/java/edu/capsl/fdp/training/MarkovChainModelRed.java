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

package edu.capsl.fdp.training;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds up the estimation of the transition matrix from the sampled transitions.
 * 
 * Loosely based on code from project https://github.com/pranab/avenir
 */
public class MarkovChainModelRed extends Reducer<Text, TransitionWritable, Text, Text> {
	
	private final Log LOG = LogFactory.getLog(MarkovChainModelRed.class);
	
	private String[] states;
	
	private Text out_matrix = new Text();
	
	@Override
	protected void setup(Reducer<Text, TransitionWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
    	states = conf.get("mc.states").split(",");
    	
    	LOG.info("Working with  " + states.length + " states : " + Arrays.toString(states));   
    	
    	// first record is the states
    	out_matrix.set(context.getConfiguration().get("mc.states"));
    	Text tmp_key = new Text("###");
    	context.write(tmp_key, out_matrix);
	}
	
	@Override
	protected void reduce(Text key, Iterable<TransitionWritable> values,
			Reducer<Text, TransitionWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		LOG.info("Processing the sampled transitions for the customer " + key.toString());
		
		TransitionMatrix trans_matrix = new TransitionMatrix(states);
		
		// Counting the occurrence of the transitions. This builds up the counting on the transition matrix.
		for (TransitionWritable transition : values) {
			trans_matrix.addTo(transition.getPresent(), transition.getFuture(), transition.getCount());
			LOG.info(transition);
		}
				
		StringBuilder sb = new StringBuilder();
		// Now, lets normalize each row, so that the values in each row sum up to one (by definition this is a probability distribution).
		trans_matrix.normalizeRows();
		// state transitions
		for (int i = 0; i < states.length; i++) {
			String val = trans_matrix.serializeRow(i);
			sb.append(val).append(";");
		}
		out_matrix.set(sb.toString());
		
		// The output format should be one transition matrix for each customer_id (you can encode the transition matrix	as a 1d array). I encoded it row-wise each row separated by ';'.
		context.write(key, out_matrix);
	}
}