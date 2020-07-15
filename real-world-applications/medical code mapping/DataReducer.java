package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, Text, Text, Text> {
 
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<String> record_ids=new ArrayList<String>();
		List<String> target_codes=new ArrayList<String>();
		for (Text value : values) {
			String[] vs=value.toString().trim().split("\t");
			if(vs[0].equals("record")) {
				record_ids.add(vs[1]);
			}
			if(vs[0].startsWith("forward")||vs[0].startsWith("backward")) {
				if(!target_codes.contains(value.toString()))
					target_codes.add(value.toString());
			}
		}
		
		for(int i=0;i<record_ids.size();i++) {
			for(int j=0;j<target_codes.size();j++) {
				context.write(new Text(""+record_ids.get(i)), new Text(""+target_codes.get(j)));
			}
			
			if(target_codes.size()==0) {
				context.write(new Text(""+record_ids.get(i)), new Text("NoMapping"));
			}
			
		}
		
	}
	
	
}

