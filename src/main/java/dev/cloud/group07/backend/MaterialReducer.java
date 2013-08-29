package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaterialReducer extends Reducer<Text, Text, NullWritable, Text> { 

	private static final NullWritable nullWritable = NullWritable.get();
	private Text valueOut = new Text();
	
	@Override
	public void reduce(Text keyIn , Iterable<Text> values , Context context) throws IOException , InterruptedException {
		
		String valueStr = "";
		for (Text value : values) {
			valueStr += value.toString() + " ";
		}
		
		// 
		valueOut.set(keyIn.toString() + "," + valueStr);
		context.write(nullWritable, valueOut);
	}
}
