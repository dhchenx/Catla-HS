package cn.edu.bjtu.cdh.bigdata.research.icd11.coding;

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
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;
import info.debatty.java.stringsimilarity.Cosine;

public class CAC_Partition {

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
		
		public String[] segWords(String content, boolean useSmart) {
			try {
				StringReader sr = new StringReader(content);
				// 参数2为是否使用智能分词
				// true：使用智能分词
				// false：使用最细粒度分词

				IKSegmenter ikSegmenter = new IKSegmenter(sr, useSmart);
				Lexeme word = null;
				String w = null;
				StringBuffer sb = new StringBuffer();
				while ((word = ikSegmenter.next()) != null) {
					w = word.getLexemeText();
					if (sb.length() > 0) {
						sb.append(" ");
					}
					sb.append(w);
				}

				// System.out.println("seg: "+sb.toString());
				return sb.toString().split(" ");
			} catch (Exception ex) {
				return content.split(" ");
			}
		}
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String fileName=getFileName(context);
			String line = value.toString();
			String[] items = line.split("\t");
			if(line.startsWith("Chapter"))
				return;
			if(items.length<2)
				return;
			
			String docId = items[0];
			String content=items[1];
			items=segWords(content,false);
			String vs = "";
			for (int i = 1; i < items.length; i++) {
				if (i != items.length - 1) {
					vs += items[i] + " ";
				} else
					vs += items[i];
			}
			
			if (!docId.isEmpty()) {
				if(fileName.startsWith("codes-"))
					context.write(new Text("codes-"+docId), new Text(vs));
				else if(fileName.startsWith("docs"))
					context.write(new Text("docs-"+docId), new Text(vs));
				
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
			if(key.toString().startsWith("docs-"))
				for(Text t:values)
					mos.write("docs", key,t);
			
			if(key.toString().startsWith("codes-"))
				for(Text t:values)
					mos.write("codes", key,t);
			
		}
		

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// 关闭多文件输出对象，刷新缓存数据
			mos.close();
		}

	}

	// user map
	public static class PartionerMapper extends Mapper<Object, Text, Text, Text> {

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
			if(items.length>=2) {
				String docId = items[0];
				String vs = items[1];
		
				context.write(new Text(), new Text(fileName + "\t" + docId+"\t"+vs));
			}
		}
	}

	public static class PartionerReducer extends Reducer<Text, Text, Text, Text> {
		// TODO Auto-generated constructor stub
		
		public double cosineSimilarity(double[] vectorA, double[] vectorB) {
			double dotProduct = 0.0;
			double normA = 0.0;
			double normB = 0.0;
			for (int i = 0; i < vectorA.length; i++) {
				dotProduct += vectorA[i] * vectorB[i];
				normA += Math.pow(vectorA[i], 2);
				normB += Math.pow(vectorB[i], 2);
			}
			return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
		}
		
		
		public List<String> getUniqueWordList(String s){
			String[] ls=s.split(" ");
			List<String> unique_words = new ArrayList<String>();
			List<Integer> unique_word_counts = new ArrayList<Integer>();
			for (int i = 1; i < ls.length; i++) {
				if (unique_words.contains(ls[i])) {
					int index = unique_words.indexOf(ls[i]);
					unique_word_counts.set(index, unique_word_counts.get(index) + 1);

				} else {
					unique_words.add(ls[i]);
					unique_word_counts.add(1);
				}
			}
			return unique_words;
		}
		

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			Iterator<Text> iterator = values.iterator();
			List<String> tlist = new ArrayList<String>();
			List<List<String>> tdlist = new ArrayList<List<String>>();
			List<List<String>> clist = new ArrayList<List<String>>();
			
			while (iterator.hasNext()) {
				String value = iterator.next().toString();
				// System.out.println("reduce="+value);
				String[] fs = value.split("\t");

				if (fs.length != 3)
					continue;

				int index = -1;
				if (tlist.contains(fs[0])) {
					index = tlist.indexOf(fs[0]);
				}
				else {
					tlist.add(fs[0]);
					 
					tdlist.add(new ArrayList<String>());
					clist.add(new ArrayList<String>());
					index = tlist.size() - 1;
				}

				tdlist.get(index).add(fs[1]);
				clist.get(index).add(fs[2]);
			}

			if (tdlist.size() == 2)
				for (int i = 0; i < tdlist.get(0).size(); i++)
					for (int j = 0; j < tdlist.get(1).size(); j++) {
						
						 String term1=clist.get(0).get(i);
						 String term2=clist.get(1).get(j);
						 Cosine cosine=new Cosine(2);
						  Map<String, Integer> profile1 = cosine.getProfile(term1);
					        Map<String, Integer> profile2 = cosine.getProfile(term2);
						 double sim=cosine.similarity(profile1, profile2);
						 if(sim>=0.2)
							 context.write(new Text(tdlist.get(0).get(i)+"\t"+tdlist.get(1).get(j)), new Text(sim+""));
			
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

		// default parameter
		if (args.length == 0) {
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/input-100", // data to be																					// partioned
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/output-parts", // output of partions
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/output-docpairs",// output doc
					"5"																			// number of partitions
			};
		}

		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);

		// set default if not exists @vars
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");

		if (!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, "CAC_Partition");

		// define the job name

		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		fs.delete(new Path(args[2]), true);
		fs.mkdirs(new Path(args[2]));

		long startTime = System.currentTimeMillis(); // 获取开始时间
		long startTime1 = System.currentTimeMillis(); // 获取开始时间
		
		// 分区job的配置
		Job partionJob = Job.getInstance(conf,appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-1]");
		partionJob.setJarByClass(CAC_Partition.class);

		partionJob.setMapperClass(DocMapper.class);

		partionJob.setOutputKeyClass(Text.class);
		partionJob.setOutputValueClass(Text.class);
		partionJob.setReducerClass(DocReducer.class);
		
		partionJob.setPartitionerClass(HashPartitioner.class);
		partionJob.setNumReduceTasks(Integer.parseInt(args[3]));

		FileInputFormat.addInputPath(partionJob, new Path(args[0]));
		
		MultipleOutputs.addNamedOutput(partionJob, "codes",TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(partionJob, "docs",TextOutputFormat.class, Text.class, Text.class);
		
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
				if(fileStatus.getPath().getName().startsWith("docs-")) {
					docList.add(fileStatus.getPath().getName());
				}else if(fileStatus.getPath().getName().startsWith("codes-")) {
					codeList.add(fileStatus.getPath().getName());
				}
				
				
			}
		}
		
	 
		List<Job> jobList=new ArrayList<Job>();
		

		for (int i = 0; i < docList.size(); i++) {
			for (int j = 0; j < codeList.size(); j++) {
				
				String suffix="";
				if(i==docList.size()-1 && j==codeList.size()-1)
					suffix="-END";
				
				Job job1 = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-("+i+","+j+")"+suffix+"]" );
				job1.setJarByClass(CAC_Partition.class);

				job1.setMapperClass(PartionerMapper.class);
				job1.setReducerClass(PartionerReducer.class);

				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Text.class);

				job1.getConfiguration().set("part_pair", docList.get(i) + "," + codeList.get(j));

				FileInputFormat.setInputDirRecursive(job1, true);// 递归输入
				FileInputFormat.setInputPathFilter(job1, TextPathFilter.class);

				FileInputFormat.addInputPath(job1, new Path(args[1]));
				FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/" + i + "_" + j));

				// 加入控制容器
				 jobList.add(job1);

				
			}
		}
		long endTime = System.currentTimeMillis(); // 获取结束时间
	
		long pre_time=endTime-startTime;
		List<Long> tlist=new ArrayList<Long>();
		// 在线程启动，记住一定要有这个
		for(int i=0;i<jobList.size();i++) {
			
			startTime = System.currentTimeMillis();
			
			jobList.get(i).waitForCompletion(true);
			
			endTime = System.currentTimeMillis();
			long job_time=endTime-startTime;
			
			tlist.add(job_time);
		}
		long max=0;
		for(int i=0;i<tlist.size();i++) {
			 if(tlist.get(i)>max) {
				 max=tlist.get(i);
			 }
		}
		
		long endTime1 = System.currentTimeMillis(); // 获取结束时间
		
		System.out.println("程序运行时间(success)： " + (pre_time+max) + "ms, total time: "+(endTime1-startTime1)+"ms.");
		 
	}

}
