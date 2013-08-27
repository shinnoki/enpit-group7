package com.example.dpap.class04.backend;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpecPairAggregationReducer extends Reducer<Text, IntWritable, NullWritable, Text> { 

	private static final NullWritable nullWritable = NullWritable.get();
	
	private Text valueOut = new Text();
	
	@Override
	public void reduce(Text keyIn , Iterable<IntWritable> values , Context context) throws IOException , InterruptedException {
		
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		
		//特定の商品のペアの出現回数をカウント
		while(iterator.hasNext()) {
			sum += iterator.next().get();
		}
		
		valueOut.set(keyIn.toString() + "," + sum);
		
		// TODO ここに、空のKeyと、 商品のペアとペアの出現回数がカンマ区切りで記録されたデータがValueとして出力されるロジックを実装する
		context.write(nullWritable, valueOut);
	}
}
