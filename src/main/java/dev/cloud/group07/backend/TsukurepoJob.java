package dev.cloud.group07.backend;

import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TsukurepoJob extends Job {
	
	// ツクレポ入力ファイルのパス (/user/root/rakuten_recipe/recipe_tsukurepo_20120705.txt)
	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.TSUKUREPO_FILE_NAME);
	
	//HDFS上に出力されるファイル「関連度分母データ」(/user/root/hadoop_exercise/3/data/denomination)
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.DENOMINATION_FILE_NAME);
	
	public TsukurepoJob() throws IOException{
		
			this.setJobName("TsukurepoJob");
			this.setJarByClass(TsukurepoJob.class);
			
			// TODO ここに、MapperクラスとReducerクラスを設定するロジックを実装する
			this.setMapperClass(TsukurepoMapper.class);
			this.setReducerClass(TsukurepoReducer.class);
			
			//　TODO ここに、中間データのKeyとValueの型を設定するロジックを実装する
			this.setMapOutputKeyClass(Text.class);
			this.setMapOutputValueClass(IntWritable.class);
			this.setOutputKeyClass(NullWritable.class);
			this.setOutputValueClass(Text.class);
			
			// TODO ここに、利用するInputFormatとOutputFormatを設定するロジックを実装する
			setInputFormatClass(TextInputFormat.class);
			setOutputFormatClass(TextOutputFormat.class);
			
			// TODO 入力ファイルと出力ファイルのパスを設定する
			TextInputFormat.addInputPath(this, inputFile);
			TextOutputFormat.setOutputPath(this, outputFile);
			
			this.setNumReduceTasks(10);
	}

}
