package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaterialMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	
	@Override
	public void map(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException {
		String[] line = valueIn.toString().split("\t");
		
		keyOut.set(line[0]);
		valueOut.set(line[1].replaceAll("\"", ""));
		context.write(keyOut, valueOut);
	}

}
