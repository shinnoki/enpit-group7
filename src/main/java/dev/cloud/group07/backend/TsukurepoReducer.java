package dev.cloud.group07.backend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TsukurepoReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
	
	private static final NullWritable nullWritable = NullWritable.get();
	private Text valueOut = new Text();
	
	@Override
	public void reduce(Text keyIn , Iterable<IntWritable> values , Context context) throws IOException , InterruptedException {
		
		// ツクレポ数をカウント
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}

		// レシピIDとツクレポ数をカンマ区切りで出力する
		valueOut.set(keyIn.toString() + "," + sum);
		context.write(nullWritable , valueOut);
	}
	
}
