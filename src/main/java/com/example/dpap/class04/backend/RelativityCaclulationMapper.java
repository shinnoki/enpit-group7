package com.example.dpap.class04.backend;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tools.ant.types.FileList.FileName;



public class RelativityCaclulationMapper extends Mapper<LongWritable , Text , Text , Text> {
	
	
	private Writer writer;
	
	@Override
	public void setup(Context context) throws IOException , InterruptedException {
		String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
		
		if(filePath.indexOf(FilePathConstants.FILE_BASE + "/" + FilePathConstants.DENOMINATION_FILE_NAME) > 0) {
			writer = new DenominationWriter();
		} else if (filePath.indexOf(FilePathConstants.FILE_BASE + "/" + FilePathConstants.NUMERATOR_FILE_NAME) > 0) {
			writer = new NumeratorWriter();
		} else {
			throw new RuntimeException("Invalid Input File : " + filePath);
		}
	}
	
	
	@Override
	public void map(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException {
		
		writer.write(keyIn, valueIn , context);
				
	}
	
	
	private interface Writer {
		public void write(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException;
	}
	private class DenominationWriter implements Writer {
		
		@Override
		public void write(LongWritable keyIn , Text valueIn , Context context) throws IOException , InterruptedException {
			String[] goodsNameAndNum = valueIn.toString().split(",");
			
			context.write(new Text(goodsNameAndNum[0] + "#d"), new Text(goodsNameAndNum[1]));
		}
		
	}
	private class NumeratorWriter implements Writer {
		
		private Text keyOut = new Text();
		private Text valueOut = new Text();
		
		@Override
		public void write(LongWritable keyIn , Text valueIn , Context context)  throws IOException , InterruptedException {
			String[] goodsPairAndNum = valueIn.toString().split(",");
			
			keyOut.set(goodsPairAndNum[0]);
			valueOut.set(goodsPairAndNum[1] + "," + goodsPairAndNum[2]);
			context.write(keyOut , valueOut);
			
			keyOut.set(goodsPairAndNum[1]);
			valueOut.set(goodsPairAndNum[0] + "," + goodsPairAndNum[2]);
					
					
		}
	}
}
