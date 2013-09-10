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
		mos = new MultipleOutputs<NullWritable, Text>(context);
		mos.write("pasta", nullWritable, new Text("{ \"data\": ["));
		mos.write("curry", nullWritable, new Text("{ \"data\": ["));
		mos.write("don", nullWritable, new Text("{ \"data\": ["));
		mos.write("sushi", nullWritable, new Text("{ \"data\": ["));
		mos.write("cake", nullWritable, new Text("{ \"data\": ["));
		super.setup(context);
	}

	@Override
	public void reduce(Text keyIn , Iterable<Text> values , Context context) throws IOException , InterruptedException {
		String keyStr = "\"ID\":\"";
		String category = "";

		if (keyIn.toString().matches(".*#(a|b)$")) {
			keyStr += keyIn.toString().substring(0, keyIn.toString().length()-2);
		} else {
			keyStr += keyIn.toString();
		}
		keyStr += "\"";
		String valuesStr = "";

        // カテゴリを見つけてくる
        String categoryPrefix = "\"smallCategory\":";
        for (Text value : values) {
        	String[] properties = value.toString().split(",");
        	for (String property : properties) {
        		if (property.startsWith(categoryPrefix)) {
        			category = property.substring(categoryPrefix.length());
        		}
        		
        		valuesStr += property + ",";
        	}
        }
        
        // 末尾の","を削除
        valuesStr = valuesStr.substring(0, valuesStr.length()-1);

        valueOut.set("{" + keyStr + "," + valuesStr + "},");

        if (category.contains("パスタ")) {
        	mos.write("pasta", nullWritable, valueOut);
        } else if (category.contains("カレー")) {
        	mos.write("curry", nullWritable, valueOut);
        } else if (category.contains("丼")) {
        	mos.write("don", nullWritable, valueOut);
        } else if (category.contains("寿司")) {
        	mos.write("sushi", nullWritable, valueOut);
        } else if (category.contains("ケーキ")) {
        	mos.write("cake", nullWritable, valueOut);
        }
	}	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// ほんとは末尾の","を消したいけど...
		mos.write("pasta", nullWritable, new Text("{}]}"));
		mos.write("curry", nullWritable, new Text("{}]}"));
		mos.write("don", nullWritable, new Text("{}]}"));
		mos.write("sushi", nullWritable, new Text("{}]}"));
		mos.write("cake", nullWritable, new Text("{}]}"));
		mos.close();
		super.cleanup(context);
	}
}
