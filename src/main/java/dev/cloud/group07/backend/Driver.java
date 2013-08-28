package dev.cloud.group07.backend;
/**
 * 
 * このモジュールは完成済みです。
 * 
 * 
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws IOException , ClassNotFoundException , InterruptedException {
		
		if(args.length == 0 || "all".equals(args[0])){
			int returnCode = 0;
			
			returnCode |= runTsukurepoJob(args);
			returnCode |= runProcessJob(args);
			returnCode |= runImagePropertyJob(args);
			
			return returnCode;
		}
		
		else if("tsukurepo".equals(args[0])) {
			return runTsukurepoJob(args);
		} else if("process".equals(args[0])) {
			return runProcessJob(args);
		} else if("property".equals(args[0])) {
			return runImagePropertyJob(args);
		}
		
		
		return -1;
	}
	
	public int runTsukurepoJob(String[] args) throws IOException , ClassNotFoundException , InterruptedException {
		Job tsukurepoJob = new TsukurepoJob();
		
		return (tsukurepoJob.waitForCompletion(true)) ? 0 : 1;
	}
	
	public int runProcessJob(String[] args) throws IOException , ClassNotFoundException , InterruptedException {
		Job processJob = new ProcessJob();
		
		return (processJob.waitForCompletion(true)) ? 0 : 1;
	}
	
	public int runImagePropertyJob(String[] args) throws IOException , ClassNotFoundException , InterruptedException {
		Job imagePropertyJob = new ImagePropertyJob();
		
		return (imagePropertyJob.waitForCompletion(true)) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		
		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
