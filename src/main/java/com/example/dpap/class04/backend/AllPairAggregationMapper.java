package com.example.dpap.class04.backend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// TODO 型パラメータを補完する。
//　ヒント：FileInputFormat系(TextInputFormatなど)を使用する場合、入力のKeyはLongWritable、ValueはText型である
public class AllPairAggregationMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final IntWritable one = new IntWritable(1);
	
	private Text keyOut = new Text();
	
	@Override
	public void map(LongWritable keyIn , Text valuein , Context context) throws IOException , InterruptedException {
		
		// 商品名,商品名のペアを「,」を区切り文字として分割
		String[] goodsPair = valuein.toString().split(",");
		
		for(String goods : goodsPair){
			
			keyOut.set(goods);
			
			// TODO ワードカウントの要領で、商品名をKeyに、「1」をValueに設定し、中間データを出力する
			context.write(keyOut, one);
			
		}
		
	}

}
