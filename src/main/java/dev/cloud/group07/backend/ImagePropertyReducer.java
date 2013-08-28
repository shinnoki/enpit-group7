package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ImagePropertyReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();
	
	private Text valueOut = new Text();
	
	@Override
	public void reduce(Text keyIn , Iterable<Text> values , Context context) throws IOException , InterruptedException {
		
		String keyStr = keyIn.toString();
		String valuesStr = values.toString();
		valueOut.set(keyStr + "," + valuesStr);
        context.write(nullWritable, valueOut);

        //Iterator<Text> iterator = values.iterator();
        //double denominator = Double.parseDouble(iterator.next().toString());
		/*
		while(iterator.hasNext()) {
		    String[] recipeInfo = iterator.next().toString().split(",");			
					
			double numerator = Double.parseDouble(numeratorGoodsAndNum[1]);
			
			double relativity = numerator/denominator;
			
			if(relativity * 1000 > 25) {
				valueOut.set(keyStr.substring(0 , keyStr.length()-2) + "," + numeratorGoodsAndNum[0] + "," + relativity);
				context.write(nullWritable, valueOut);
			}
		}
		*/
	}	
}
