package cn.edu.bjtu.cdh.bigdata.research.icd11.mapping;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

/*
 * forward_1-1   M->1 (from ICD-10 to ICD11) 多个ICD-10编码映射到一个ICD-11编码中

backward_1-1 M->1 (from ICD11 to ICD10) 多个ICD11编码映射到一个ICD10编码中

forward_M-M M->M (from ICD-10 to ICD11)，多个ICD10编码映射到多个ICD-11编码中
 */

public class ICD11Mapping extends Configuration implements Tool {

	@Override
	public void setConf(Configuration conf) {

	}

	@Override
	public Configuration getConf() {
		return new Configuration();
	}

	public static String getIP(String hdfs_url) {
		String[] fs = hdfs_url.split(":");
		return fs[1].replace("/", "");
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length <= 0) {
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/icd-transition/input",
					"hdfs://192.168.159.132:9000/data/cdh/icd-transition/output-mappings",
					"hdfs://192.168.159.132:9000/data/cdh/icd-transition/output-docs",
					"hdfs://192.168.159.132:9000/data/cdh/icd-transition/output-docs+nomapping", };
		}

		Configuration conf = new Configuration();

		// obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);

		// set default if not exists @vars
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");

		if (!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, "ICD11Mapping");

		// define the job name

		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		fs.delete(new Path(args[2]), true);
		fs.delete(new Path(args[3]), true);

		Job job = Job.getInstance(conf,
				appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-1]");
		job.setJarByClass(ICD11Mapping.class);

		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);

		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// doc
		Job job2 = Job.getInstance(conf,
				appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-2]");;
		job2.setJarByClass(ICD11Mapping.class);

		job2.setMapperClass(DocMapper.class);
		job2.setReducerClass(DocReducer.class);

		job2.setOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);

		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		//no mapping
		
		Job job3 = Job.getInstance(conf,
				appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-3-END]");;
			job3.setJarByClass(ICD11Mapping.class);
		 
				job3.setMapperClass(NoCodingMapper.class);
				job3.setReducerClass(NoCodingReducer.class);
				
				job3.setOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				
				FileInputFormat.addInputPath(job3, new Path(args[0]));
				FileInputFormat.addInputPath(job3, new Path(args[2]));
				FileOutputFormat.setOutputPath(job3, new Path(args[3]));

	
		
		if (job.waitForCompletion(true)) {
			if (job2.waitForCompletion(true)) {
				 job3.waitForCompletion(true);
			}
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ToolRunner.run(conf, new ICD11Mapping(), args); // 调用run方法
	}
}
