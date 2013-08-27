package dev.cloud.group07.backend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TsukurepoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static final IntWritable one = new IntWritable(1);
	
	private Text keyOut = new Text();
	
	@Override
	public void map(LongWritable keyIn , Text valuein , Context context) throws IOException , InterruptedException {
		// 改行後に入ってくるものを無視
		if (valuein.toString().startsWith("\\n")) return;

		// レシピID ユーザID おすすめコメント オーナーコメント　作成日時 (Tab区切り)
		String[] line = valuein.toString().split("\t");
		keyOut.set(line[0]);
		context.write(keyOut, one);
	}

}
