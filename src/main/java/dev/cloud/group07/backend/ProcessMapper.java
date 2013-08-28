package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProcessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final IntWritable one = new IntWritable(1);
	
	private Text keyOut = new Text();
	
	@Override
	public void map(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException {
		String[] line = valueIn.toString().split("\t");
		keyOut.set(line[0]);
		context.write(keyOut, one);
	}

}
