package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterators;

public class DocReducer extends Reducer<Text, Text, Text, Text> {
 
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<String> vlist=new ArrayList<String>();
		for (Text value : values) {
			String v=value.toString();
		  
		 vlist.add(v);
		}
		
		// detect if there is nomapping flag
		int count=0;
		String temp="";
		for (String value : vlist) {
			count++;
			temp=value.toString();
		}
	 
		if(count==1) {
			
			if(temp.equalsIgnoreCase("NoMapping")) {
				
				context.write(key, new Text(temp));
				
				return;
			}
			
		}

		// without nomapping flag
		List<String> relations=new ArrayList<String>();
		List<String> codes=new ArrayList<String>();
		for (String value : vlist) {
			String[] vs=value.trim().split("\t");
			
				if(vs.length<2)continue;
				
				relations.add(vs[0]);
				codes.add(vs[1]);
		 
		}
		
		String cs="";
		
		List<String> unique_codes=new ArrayList<String>();
		for(int i=0;i<codes.size();i++) {
			if(!unique_codes.contains(codes.get(i))) {
				unique_codes.add(codes.get(i));
				cs+=codes.get(i)+";";
			}
		}
		
		if(cs.endsWith(";"))
			cs=cs.substring(0, cs.length()-1);
		
		int f_M_1=0;
		int f_M_M=0;
		int b_M_1=0;
	
		for(int i=0;i<relations.size();i++) {
			 if(relations.get(i).equals("forward_M-1"))
				 f_M_1++;
			 if(relations.get(i).equals("forward_M-M"))
				 f_M_M++;
			 if(relations.get(i).equals("backward_M-1"))
				 b_M_1++;
		}
		
		
		context.write(key, new Text(""+cs+"\t"+unique_codes.size()+"\t"+f_M_1+"\t"+f_M_M+"\t"+b_M_1));

		
	}
}

