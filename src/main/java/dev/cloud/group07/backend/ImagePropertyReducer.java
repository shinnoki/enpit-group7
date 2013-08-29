package dev.cloud.group07.backend;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ImagePropertyReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();	
	private Text valueOut = new Text();
	private MultipleOutputs<NullWritable, Text> mos;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		context.write(nullWritable, new Text("var obj = ["));
		mos = new MultipleOutputs<NullWritable, Text>(context);
		super.setup(context);
	}

	@Override
	public void reduce(Text keyIn , Iterable<Text> values , Context context) throws IOException , InterruptedException {
		String keyStr = "ID:\"";
		String category = "";

		if (keyIn.toString().matches(".*#(a|b)$")) {
			keyStr += keyIn.toString().substring(0, keyIn.toString().length()-2);
		} else {
			keyStr += keyIn.toString();
		}
		keyStr += "\"";
		String valuesStr = "";

        Iterator<Text> iterator = values.iterator();
        
        // カテゴリを見つけてくる
        String categoryPrefix = "smallCategory:";
        for (Text value : values) {
        	String[] properties = value.toString().split(",");
        	for (String property : properties) {
        		if (property.startsWith(categoryPrefix)) {
        			category = property.substring(categoryPrefix.length());
        		}
        		
        		valuesStr += property + ",";
        	}
        }

        valueOut.set("{" + keyStr + "," + valuesStr + "},");
        // context.write(nullWritable, valueOut);
        if (category.contains("パスタ")) {
        	mos.write("pasta", nullWritable, valueOut);
        }
	}	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		mos.close();
		context.write(nullWritable, new Text("];"));
		super.cleanup(context);
	}
}
