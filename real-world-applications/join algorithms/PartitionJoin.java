package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

public class PartitionJoin {

	// user map
	public static class DocMapper extends Mapper<Object, Text, Text, Text> {

		private String getFileName(Context context) throws IOException {
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
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String fileName = getFileName(context);
			String line = value.toString();

			String[] items = line.split("\t");
			if (line.isEmpty() || items[0].contains("_ID"))
				return;

			String docId = items[0];
			String content = items[1];

			String vs = "";
			for (int i = 1; i < items.length; i++) {
				if (i != items.length - 1) {
					vs += items[i] + " ";
				} else
					vs += items[i];
			}

			if (!docId.isEmpty()) {
				if (fileName.startsWith("codes"))
					context.write(new Text("codes-" + docId), new Text(vs));
				else if (fileName.startsWith("docs"))
					context.write(new Text("docs-" + docId), new Text(vs));

			}

		}
	}

	public static class HashPartitioner<K, V> extends Partitioner<K, V> {

		/** Use {@link Object#hashCode()} to partition. */
		public int getPartition(K key, V value, int numReduceTasks) {

			// 默认使用key的hash值与上int的最大值，避免出现数据溢出 的情况
			return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}

	}

	public static class PPartition extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text arg0, Text arg1, int arg2) {
			/**
			 * 自定义分区，实现长度不同的字符串，分到不同的reduce里面
			 * 
			 * 现在只有3个长度的字符串，所以可以把reduce的个数设置为3 有几个分区，就设置为几
			 */

			String key = arg0.toString();
			if (key.length() == 1) {
				return 1 % arg2;
			} else if (key.length() == 2) {
				return 2 % arg2;
			} else if (key.length() == 3) {
				return 3 % arg2;
			}
			return 0;
		}
	}

	public static class DocReducer extends Reducer<Text, Text, Text, Text> {
		// TODO Auto-generated constructor stub
		private MultipleOutputs<Text, Text> mos = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 创建多文件输出对象
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (key.toString().startsWith("docs-"))
				for (Text t : values)
					mos.write("docs", key, t);

			if (key.toString().startsWith("codes-"))
				for (Text t : values)
					mos.write("codes", key, t);

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// 关闭多文件输出对象，刷新缓存数据
			mos.close();
		}

	}

	// user map
	public static class PartitionMapper extends Mapper<Object, Text, Text, Text> {

		private String getFileName(Context context) throws IOException {
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
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String fileName = getFileName(context);

			String line = value.toString();
			String[] fs = line.split("\t");
			if(line.isEmpty()||line.length()<=2)
				return;
	 
		     context.write(new Text(fs[1]), new Text(fileName+"\t"+fs[0]));
				 
		 
			 
		}
		
		
	}

	public static class PartitionReducer extends Reducer<Text, Text, Text, Text> {
		// TODO Auto-generated constructor stub

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			Iterator<Text> iterator = values.iterator();
			
		

			List<String> dlist = new ArrayList<String>();
			List<String> clist = new ArrayList<String>();
			 
			while (iterator.hasNext()) {
				String value = iterator.next().toString();
				// System.out.println("reduce="+value);
				String[] fs = value.split("\t");
 
				if (fs[0].startsWith("codes")) {
					clist.add(fs[1]);
				}

				if (fs[0].startsWith("docs")) {
					dlist.add(fs[1]);
				}

			}

			for (int i = 0; i < dlist.size(); i++)
				for (int j = 0; j < clist.size(); j++) {
					context.write(key, new Text(dlist.get(i).replace("docs-", "") + "\t" + clist.get(j).replace("codes-", "")));
				}

		}

	}

	static class TextPathFilter extends Configured implements PathFilter {
		@Override
		public boolean accept(Path path) {
			FileSystem fs;
			try {
				fs = FileSystem.get(getConf());
				FileStatus fstatus = fs.getFileStatus(path);
				List<String> lstName = new ArrayList<String>();
				String[] pps = getConf().get("part_pair").split(",");
				for (int i = 0; i < pps.length; i++)
					lstName.add(pps[i]);

				if (fstatus.isDirectory()) { // 是目录的话返回true
					return true;
				}

				System.out.println("fileName = " + fstatus.getPath().getName());

				if (fstatus.isFile() && lstName.contains(fstatus.getPath().getName())) { // 是文件的话且满足过滤条件返回true
					return true;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			return false;
		}

	}

	public static String getIP(String hdfs_url) {
		String[] fs = hdfs_url.split(":");
		return fs[1].replace("/", "");
	}

	// hadoop jar cacjoin.jar cn.edu.bjtu.cdh.bigdata.cacjoin.MapJoin
	// hdfs://192.168.100.74:9000/data/cdh/icd-cac-join/input-icd
	// hdfs://192.168.100.74:9000/data/cdh/icd-cac-join/input-data
	// hdfs://192.168.100.74:9000/data/cdh/icd-cac-join/output-map inner

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// default parameter
		if (args.length == 0) {
			String jobName = "MutiplePartitionJoin";
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/research/join/input", // data to be partioned
					"hdfs://192.168.159.132:9000/data/cdh/research/join/output-partitionjoin-tmp", // output of partions
					"hdfs://192.168.159.132:9000/data/cdh/research/join/output-partitionjoin", // output doc
					"2", // number of partitions
					"@traceId=" + System.currentTimeMillis(),
					"@jobName=" + jobName
			};
		}

		// basic configuration
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);
		
		 

		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		fs.delete(new Path(args[2]), true);
		

		// 分区job的配置
		Job partionJob = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");
		
		partionJob.setJarByClass(PartitionJoin.class);

		partionJob.setMapperClass(DocMapper.class);

		partionJob.setOutputKeyClass(Text.class);
		partionJob.setOutputValueClass(Text.class);
		partionJob.setReducerClass(DocReducer.class);

		partionJob.setPartitionerClass(HashPartitioner.class);
		partionJob.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(partionJob, new Path(args[0]));

		MultipleOutputs.addNamedOutput(partionJob, "codes", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(partionJob, "docs", TextOutputFormat.class, Text.class, Text.class);

		FileOutputFormat.setOutputPath(partionJob, new Path(args[1]));

		partionJob.waitForCompletion(true);

		// 获得所有分区文件
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[1]), true);
		List<String> docList = new ArrayList<String>();
		List<String> codeList = new ArrayList<String>();

		while (fileStatusListIterator.hasNext()) {
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			System.out.println("path=" + fileStatus.getPath().getName());
			if (!fileStatus.getPath().getName().startsWith("_SUCCESS")) {
				if (fileStatus.getPath().getName().startsWith("docs-")) {
					docList.add(fileStatus.getPath().getName());
				} else if (fileStatus.getPath().getName().startsWith("codes-")) {
					codeList.add(fileStatus.getPath().getName());
				}

			}
		}

		// 开始创建针对不同分区对的job
		JobControl jc = new JobControl("myctrl");

		for (int i = 0; i < docList.size(); i++) {
			for (int j = 0; j < codeList.size(); j++) {

				Job job1 = Job.getInstance(conf, "generate doc pairs: doc" + i + ", code" + j);
				job1.setJarByClass(PartitionJoin.class);

				job1.setMapperClass(PartitionMapper.class);
				job1.setReducerClass(PartitionReducer.class);

				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);

				job1.getConfiguration().set("part_pair", docList.get(i) + "," + codeList.get(j));

				FileInputFormat.setInputDirRecursive(job1, true);// 递归输入
				FileInputFormat.setInputPathFilter(job1, TextPathFilter.class);

				FileInputFormat.addInputPath(job1, new Path(args[1]));
				FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/" + i + "_" + j));

				// 加入控制容器
				ControlledJob ctrljob1 = new ControlledJob(conf);
				ctrljob1.setJob(job1);
				jc.addJob(ctrljob1);

			}
		}

		// 在线程启动，记住一定要有这个
		Thread t = new Thread(jc);
		t.start();

		long startTime = System.currentTimeMillis(); // 获取开始时间

		Thread jcThread = new Thread(jc);
		jcThread.start();
		while (true) {
			if (jc.allFinished()) {
				System.out.println(jc.getSuccessfulJobList());
				jc.stop();
				long endTime = System.currentTimeMillis(); // 获取结束时间
				System.out.println("程序运行时间(success)： " + (endTime - startTime) + "ms");
				break;
			}
			if (jc.getFailedJobList().size() > 0) {
				System.out.println(jc.getFailedJobList());
				jc.stop();
				long endTime = System.currentTimeMillis(); // 获取结束时间
				System.out.println("程序运行时间：(failed) " + (endTime - startTime) + "ms");
				break;
			}
		}
	}

}
