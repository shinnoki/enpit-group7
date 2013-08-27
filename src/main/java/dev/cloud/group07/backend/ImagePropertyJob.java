package dev.cloud.group07.backend;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ImagePropertyJob  extends Job {
	
		
	private static final Path denominationtFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.DENOMINATION_FILE_NAME);
	private static final Path numerationFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.NUMERATOR_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.RELATED_GOODS_FILE_NAME);

	public ImagePropertyJob() throws IOException{
		this.setJobName("RelativityCalculationJob");
		this.setJarByClass(ImagePropertyJob.class);
		
		this.setMapperClass(ImagePropertyMapper.class);
		this.setReducerClass(ImagePropertyReducer.class);
		
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(Text.class);
		this.setOutputKeyClass(NullWritable.class);
		
		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(this, denominationtFile);
		FileInputFormat.addInputPath(this, numerationFile);
		FileOutputFormat.setOutputPath(this, outputFile);
		
		this.setPartitionerClass(RelativityCalculationPartitioner.class);
		this.setSortComparatorClass(RelativityCalculationSortComparator.class);
		this.setGroupingComparatorClass(RelativityCalculationGroupComparator.class);
		
		this.setNumReduceTasks(10);
	}
		
	
	private static class RelativityCalculationPartitioner extends HashPartitioner<Text, Text> {
		
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			
						
			String strKey = key.toString();
			
			if(strKey.endsWith("#d")) {
				int idx = strKey.lastIndexOf("#d");
				return super.getPartition(new Text(strKey.substring(0 , idx)) , value , numReduceTasks);
			} else {
				return super.getPartition(key, value, numReduceTasks);
			}
			
		}
	}
	
	
	private static class RelativityCalculationGroupComparator extends WritableComparator {
		
		
		public RelativityCalculationGroupComparator() { 
			
			super(Text.class);
			
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2,
				int s2, int l2) {
			
			try{
				
				int n1 = WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = WritableUtils.decodeVIntSize(b2[s2]);
				
				String a = Text.decode(b1 , s1+n1 , l1-n1);
				String b = Text.decode(b2 , s2+n2 , l2-n2);
				
				return compare(new Text(a) , new Text(b));
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			
		}
		
		@Override
		public int compare(WritableComparable a , WritableComparable b) {
			
			if(a == b) {
				return 0;
			}
			
						
			Text left = (Text)a;
			Text right = (Text)b;
									
			if(left.toString().endsWith("#d")) {
				
				String leftStr = left.toString();
				int flagIdx = leftStr.lastIndexOf("#d");
				
				return -new Text(leftStr.substring(0 , flagIdx)).compareTo(right);
				
			} else if (right.toString().endsWith("#d")) {
				
				String rightStr = right.toString();
				int flagIdx = rightStr.lastIndexOf("#d");
				
				return -left.compareTo(new Text(rightStr.substring(0 , flagIdx)));
			} else {
				return -left.compareTo(right);
			}
		}
	}

	
	private static class RelativityCalculationSortComparator extends WritableComparator {
		
		
		public RelativityCalculationSortComparator() { 
			
			super(Text.class);
			
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2,
				int s2, int l2) {
			
			try{
				
				int n1 = WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = WritableUtils.decodeVIntSize(b2[s2]);
				
				String a = Text.decode(b1 , s1+n1 , l1-n1);
				String b = Text.decode(b2 , s2+n2 , l2-n2);
				
				return compare(new Text(a) , new Text(b));
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
			
		}
		
		
		@Override
		public int compare(WritableComparable a , WritableComparable b) {
			
			if(a == b) {
				return 0;
			}
			
						
			return -a.compareTo(b);
		}
	}
}
