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

	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.PROCESS_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.PROCESS_COUNT_FILE_NAME);
	
	public ProcessJob() throws IOException{
		
			this.setJobName("ProcessJob");
			this.setJarByClass(ProcessJob.class);
			
			// MapperクラスとReducerクラスをセット
			this.setMapperClass(ProcessMapper.class);
			this.setReducerClass(ProcessReducer.class);
			
			this.setMapOutputKeyClass(Text.class);
			this.setMapOutputValueClass(IntWritable.class);
			
			// 出力データのKeyとValueのクラスを設定
			this.setOutputKeyClass(NullWritable.class);
			this.setOutputValueClass(Text.class);
			
			this.setInputFormatClass(TextInputFormat.class);
			this.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(this, inputFile);
			FileOutputFormat.setOutputPath(this, outputFile);
			
			// Reduceタスクを10並列で実行
			this.setNumReduceTasks(10);
	}
}
