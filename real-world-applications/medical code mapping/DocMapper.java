package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

//map medical records
public class DocMapper extends Mapper<LongWritable, Text, Text, Text> {

	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		
		
		String line = value.toString().trim();

	 
			
			if (line.length() > 0) {
				//has mapping
				String[] fs = line.split("\t");
				if(fs.length==3) {
				context.write(new Text(fs[0]), new Text(fs[1] + "\t" + fs[2]));
				}
				//No Mapping
				if(fs.length==2) {
					context.write(new Text(fs[0]), new Text(fs[1] ));
				}

			}
		

		 

	}
}
