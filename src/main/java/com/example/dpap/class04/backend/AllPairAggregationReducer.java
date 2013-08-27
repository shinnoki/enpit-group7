package com.example.dpap.class04.backend;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// TODO 型パラメータを補完する
// ヒント：Reducerの入力データのKeyとValueの型は、Mapperの出力KeyとValueの型と一致させる
public class AllPairAggregationReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
	
	private static final NullWritable nullWritable = NullWritable.get();
	
	private Text valueOut = new Text();
	
	@Override
	public void reduce(Text keyIn , Iterable<IntWritable> values , Context context) throws IOException , InterruptedException {
		
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		
		while(iterator.hasNext()) {
			// TODO ここに、keyInで示される商品が何個出現したかカウントするロジックを実装する
			sum += iterator.next().get();
		}
		
		// TODO ここに、商品名と出現回数をカンマ区切りで出力するロジックを実装する
		// ヒント：TextオブジェクトのtoStringメソッドで文字列に変換可能
	
		valueOut.set(keyIn.toString()+","+sum);
		
		context.write(nullWritable , valueOut);
		
	}
	
}
