package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

//map medical records
public class DataMapper extends Mapper<LongWritable, Text, Text, Text> {

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

	private String ToStandardizedICD10Code(String icd10code) {
		 
		String newCode="";
		if(icd10code.contains("."))
		{
			
			newCode=icd10code.substring(0,icd10code.indexOf(".")+1);
			String suffix=icd10code.substring(icd10code.indexOf(".")+1);
			if(suffix.length()>1)
				suffix=suffix.substring(0,1);
			newCode=newCode+suffix;
		}else {
			newCode=icd10code;
		}
		
		//System.out.println(newCode);
		return newCode;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String fileName = getFilePath(context);

		
		String line = value.toString().trim();

		if (fileName.startsWith("medical_records")) {
			System.out.println("filename=" + fileName);
			
			if (line.length() > 0) {
				String[] fs = line.split("\t");
				if (fs.length<2)return;
				String newCode=ToStandardizedICD10Code(fs[1]);
				context.write(new Text(newCode), new Text("record" + "\t" + fs[0] ));

			}
		}

		if (fileName.equals("forward_M-1.txt")) {
			System.out.println("filename=" + fileName);
			if (line.length() > 0) {
				String[] fs = line.split("\t");
				if (fs.length<2)return;
				if(fs.length==2 && !fs[0].trim().isEmpty())
				context.write(new Text(fs[0]), new Text("forward_M-1" + "\t" + fs[1]));

			}
		}
		
		if (fileName.equals("forward_M-M.txt")) {
			System.out.println("filename=" + fileName);
			if (line.length() > 0) {
				String[] fs = line.split("\t");
				
				if (fs.length<2)return;
				if(fs.length==2 && !fs[0].trim().isEmpty())
				context.write(new Text(fs[0]), new Text("forward_M-M" + "\t" + fs[1]));

			}
		}

		if (fileName.equals("backward_M-1.txt")) {
			System.out.println("filename=" + fileName);
			if (line.trim().length() > 0) {
				String[] fs = line.split("\t");
				if(fs.length==2 && !fs[0].trim().isEmpty())
					context.write(new Text(fs[1]), new Text("backward_M-1" + "\t" + fs[0]));
			}
		}

	}
}
