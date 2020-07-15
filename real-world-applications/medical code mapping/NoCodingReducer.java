package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NoCodingReducer extends Reducer<Text, Text, Text, Text> {
 
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
				
		boolean isNoCoding=true;
		
		for (Text value : values) {
			
			String v=value.toString();
			
			if(!v.isEmpty()) {
				isNoCoding=false;
				context.write(key, new Text(v));
			} 
		 
		}
		
	
		if(isNoCoding) {
			context.write(key, new Text("NoMapping"));
		}
		
		
		

		
	}
}

