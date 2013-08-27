package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// TODO 型パラメータを補完する
// ヒント：Mapperの出力データのKeyとValueの型は、ReducerのKeyとValueの型と一致させる
public class ProcessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final IntWritable one = new IntWritable(1);
	
	private Text keyOut = new Text();
	
	@Override
	public void map(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException {
		
		String[] goodsPair = valueIn.toString().split(",");
		
		// TODO ここに、商品のペアを昇順にソートするロジックを実装する
		// ヒント：Arraysクラスのsortメソッドが利用できる
		Arrays.sort(goodsPair);
		
		keyOut.set(goodsPair[0] + "," + goodsPair[1]);
		
		// TODO ここに、昇順でカンマ区切りの商品ペアをKey、「1」をValueとして中間データを出力するロジックを実装する
		context.write(keyOut, one);
	}

}
