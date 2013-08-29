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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ImagePropertyJob  extends Job {
		
	private static final Path processCountFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.PROCESS_COUNT_FILE_NAME);
	private static final Path tsukurepoCountFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.TSUKUREPO_COUNT_FILE_NAME);
	private static final Path recipeAllFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.ALL_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE + "/" + FilePathConstants.EVALUATION_FILE_NAME);

	public ImagePropertyJob() throws IOException{
		this.setJobName("ImagePropertyJob");
		this.setJarByClass(ImagePropertyJob.class);
		
		this.setMapperClass(ImagePropertyMapper.class);
		this.setReducerClass(ImagePropertyReducer.class);
		
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(Text.class);
		this.setOutputKeyClass(NullWritable.class);
		this.setOutputValueClass(Text.class);
		
		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(this, processCountFile);
		FileInputFormat.addInputPath(this, tsukurepoCountFile);
		FileInputFormat.addInputPath(this, recipeAllFile);
		FileOutputFormat.setOutputPath(this, outputFile);
		
		MultipleOutputs.addNamedOutput(this, "pasta",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(this, "japanese",
				TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(this, "italian",
				TextOutputFormat.class, NullWritable.class, Text.class);
		
		this.setPartitionerClass(ImagePropertyPartitioner.class);
		this.setSortComparatorClass(ImagePropertySortComparator.class);
		this.setGroupingComparatorClass(ImagePropertyGroupComparator.class);
		
		this.setNumReduceTasks(10);
	}
		
	
	private static class ImagePropertyPartitioner extends HashPartitioner<Text, Text> {
		
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String strKey = key.toString();
			
			if(strKey.endsWith("#a")) {
				int idx = strKey.lastIndexOf("#a");
				return super.getPartition(new Text(strKey.substring(0 , idx)) , value , numReduceTasks);
			} else if(strKey.endsWith("#b")) {
                int idx = strKey.lastIndexOf("#b");
                return super.getPartition(new Text(strKey.substring(0 , idx)) , value , numReduceTasks);
			} else {
				return super.getPartition(key, value, numReduceTasks);
			}
			
		}
	}
	
	
	private static class ImagePropertyGroupComparator extends WritableComparator {
		
		
		public ImagePropertyGroupComparator() { 
			
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
									
			if(left.toString().matches(".*#(a|b)$") && !right.toString().matches(".*#(a|b)$")) {
				
				String leftStr = left.toString();
				int flagIdx = leftStr.lastIndexOf("#");
				
				return -(new Text(leftStr.substring(0 , flagIdx)).compareTo(right));
				
			} else if (!left.toString().matches(".*#(a|b)$") && right.toString().matches(".*#(a|b)$")) {
				
				String rightStr = right.toString();
				int flagIdx = rightStr.lastIndexOf("#");
				
				return -(left.compareTo(new Text(rightStr.substring(0 , flagIdx))));
			} else if (left.toString().matches(".*#(a|b)$") && right.toString().matches(".*#(a|b)$")) {
				
				String leftStr = left.toString();
				int leftFlagIdx = leftStr.lastIndexOf("#");
				String rightStr = right.toString();
				int rightFlagIdx = rightStr.lastIndexOf("#");
				
				return -(new Text(leftStr.substring(0 , leftFlagIdx)).compareTo(new Text(rightStr.substring(0 , rightFlagIdx))));
			} else {
				return -(left.compareTo(right));
			}
		}
	}

	
	private static class ImagePropertySortComparator extends WritableComparator {
		
		
		public ImagePropertySortComparator() { 
			
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
