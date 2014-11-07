package edu.capsl.fdp.training;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MarkovChainModelRed extends Reducer<TransitionWritable, IntWritable, NullWritable, Text> {
	
	@Override
	protected void setup(
			Reducer<TransitionWritable, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	@Override
	protected void reduce(
			TransitionWritable key,
			Iterable<IntWritable> value,
			Reducer<TransitionWritable, IntWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key + " " + value);
	}
}