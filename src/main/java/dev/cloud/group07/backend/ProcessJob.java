package dev.cloud.group07.backend;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ProcessJob extends Job{

	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.GOODS_PAIR_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.NUMERATOR_FILE_NAME);
	
	public ProcessJob() throws IOException{
		
			this.setJobName("SpecPairAggregationJob");
			this.setJarByClass(ProcessJob.class);
			
			// TODO ここに、MapperクラスとReducerクラスをセットするロジックを実装する
			this.setMapperClass(ProcessMapper.class);
			this.setReducerClass(ProcessReducer.class);
			
			this.setMapOutputKeyClass(Text.class);
			this.setMapOutputValueClass(IntWritable.class);
			
			// TODO ここに、出力データのKeyとValueのクラスを設定するロジックを実装する
			this.setOutputKeyClass(NullWritable.class);
			this.setOutputValueClass(Text.class);
			
			this.setInputFormatClass(TextInputFormat.class);
			this.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(this, inputFile);
			FileOutputFormat.setOutputPath(this, outputFile);
			
			// TODO ここに、Reduceタスクを10並列で実行するようなロジックを実装する
			this.setNumReduceTasks(10);
	}
}
