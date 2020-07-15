package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//map medical records
public class NoCodingMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String getFilePath(Context context) throws IOException {
		// FileSplit fileSplit = (FileSplit) context.getInputSplit();
		InputSplit split = context.getInputSplit();
		Class<? extends InputSplit> splitClass = split.getClass();
		FileSplit fileSplit = null;
		if (splitClass.equals(FileSplit.class)) {
			fileSplit = (FileSplit) split;
		} else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
			// begin reflection hackery...
			try {
				Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
				getInputSplitMethod.setAccessible(true);
				fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
			} catch (Exception e) {
				// wrap and re-throw error
				throw new IOException(e);
			}
			// end reflection hackery
		}
		return fileSplit.getPath().getName();
	}


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String fileName = getFilePath(context);

		if (!fileName.startsWith("forward")&&!fileName.startsWith("backward")) {
			
		String line = value.toString().trim();
		String[] fs = line.split("\t");
		if (line.length()>0) {
				//original
				if(fs.length==2) {
					context.write(new Text(fs[0]), new Text(""));
				}
				
				//has coding
				if(fs.length>2) {
				
					String doc_id=fs[0];
					String new_line=line.substring(doc_id.length());
					context.write(new Text(doc_id), new Text(new_line));
				}

			}
		}
		
	}
}
