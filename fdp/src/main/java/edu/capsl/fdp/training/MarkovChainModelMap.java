package edu.capsl.fdp.training;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarkovChainModelMap extends Mapper<Object, Text, TransitionWritable, IntWritable> {

	private final Log LOG = LogFactory.getLog(MarkovChainModelMap.class);
	
	private TransitionWritable out_key = new TransitionWritable();
	private IntWritable out_value = new IntWritable();
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, TransitionWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		// split by space or tab
		String[] tokens = value.toString().split("[[ ]*\t]");
		
		if (tokens.length == 3) {
			
			out_key.setPresent(tokens[0]);
			out_key.setFuture(tokens[1]);
			
			out_value.set(Integer.parseInt(tokens[2]));
						
			context.write(out_key, out_value);
			
			LOG.info("'" + out_key + "' : " + out_value);
			
		} else
			LOG.warn("The number of tokens is not 3, instead is " + tokens.length + " for the string " + value);
		
	}
}
