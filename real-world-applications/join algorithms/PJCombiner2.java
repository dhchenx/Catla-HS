package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PJCombiner2 {

	static enum PartitionNums {
		L_NUM,
		R_NUM
	}
	
	static enum LineCounters {
		L_LINE_COUNT,
		R_LINE_COUNT
	}
	
	// user map
	public static class LCMapper extends Mapper<Object, Text, Text, Text> {
		
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
			
			String fileName=getFileName(context);
			
			if(fileName.startsWith("docs")) {
			context.getCounter(LineCounters.L_LINE_COUNT).increment(1);
			}
			if(fileName.startsWith("codes")) {
				context.getCounter(LineCounters.R_LINE_COUNT).increment(1);
		}
		}
		
	}

	
	// user map
	public static class DocJoinMapper extends Mapper<Object, Text, Text, Text> {
 
		static int L_Block=-1;//每个分区的记录数目
		static int R_Block=-1;
		static int N=-1;//总分区
		public void setup(Context context) throws IOException {

			L_Block=Integer.parseInt(context.getConfiguration().get("L_Block"));
			R_Block=Integer.parseInt(context.getConfiguration().get("R_Block"));
			N=Integer.parseInt(context.getConfiguration().get("N"));
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString().trim();
			String[] fs = line.split("\t");

			if(line.isEmpty()|| fs[0].contains("_ID"))
				return;
			
			if(fs.length<2)
				return;
			
			//每处理一行，增加一个
			context.getCounter(PartitionNums.L_NUM).increment(1);
			
			//获取当前行数
			long l_num=context.getCounter(PartitionNums.L_NUM).getValue();
			
			//计算当前的第几个分区
			long current_p_num=(long)((l_num-1)/L_Block);
			
			//更新全局L当前第几个分区
			//context.getCounter(PartitionNums.L_Partition_NUM).setValue(current_p_num);
			System.out.println("L"+current_p_num+"\t"+"R"+"{i}");
			System.out.println("L_NUM="+l_num+", L_BLOCK="+L_Block);
			System.out.println("N="+N);
			
			//获取R的第几个分区
			//System.out.println("line="+line);
			for(int i=0;i<N;i++) {
				
				context.write(new Text("L"+current_p_num+"\t"+"R"+i+"\t"+fs[1]), new Text("docs\t"+fs[0]+"\t"+fs[1]));
				//System.out.println("L"+current_p_num+"\t"+"R"+i+"\t"+fs[1]);
				//System.out.println("docs\t"+fs[0]+"\t"+fs[1]);
				//System.out.println("");
				
			}
		}
	}

	// city map
	public static class CodeJoinMapper extends Mapper<Object, Text, Text, Text> {
		// TODO Auto-generated constructor st
	 
		static int L_Block=-1;//每个分区的记录数目
		static int R_Block=-1;
		static int M=-1;//总分区
		public void setup(Context context) throws IOException {

			L_Block=Integer.parseInt(context.getConfiguration().get("L_Block"));
			R_Block=Integer.parseInt(context.getConfiguration().get("R_Block"));
			M=Integer.parseInt(context.getConfiguration().get("M"));
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString().trim();

			String[] fs = line.split("\t");

			if(line.isEmpty()|| fs[0].contains("_ID"))
				return;

			if(fs.length<2)
				return;
			
			//每处理一行，增加一个
			context.getCounter(PartitionNums.R_NUM).increment(1);
			
			//获取当前行数
			long r_num=context.getCounter(PartitionNums.R_NUM).getValue();
			
			//计算当前的第几个分区
			long current_p_num=(long)((r_num-1)/R_Block);
			
			//更新全局L当前第几个分区
		//	context.getCounter(PartitionNums.R_Partition_NUM).setValue(current_p_num);
			
			System.out.println("L{i}\t"+"R"+current_p_num);
			System.out.println("R_NUM="+r_num+", R_BLOCK="+R_Block);
			System.out.println("M="+M);
			
			for(int i=0;i<M;i++) {
				context.write(new Text("L"+i+"\t"+"R"+current_p_num+"\t"+fs[1]),  new Text("codes\t"+fs[0]+"\t"+fs[1]));
				//System.out.println("L"+i+"\t"+"R"+current_p_num+"\t"+fs[1]);
				//System.out.println("codes\t"+fs[0]+"\t"+fs[1]);
				//System.out.println("");
			}

		}

	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		// TODO Auto-generated constructor stub
		// Join type:{inner,leftOuter,rightOuter,fullOuter,anti}
		private String joinType = null;
		private static final Text EMPTY_VALUE = new Text("");
		private List<Text> listA = new ArrayList<Text>();
		private List<Text> listB = new ArrayList<Text>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 获取join的类型
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			listA.clear();
			listB.clear();
			
			System.out.println("Reducer:\n"+key.toString());
			
			Iterator<Text> iterator = values.iterator();
			
			while (iterator.hasNext()) {
				String value = iterator.next().toString();
				
				String[] fs=null;
				
				if(value.contains(";;")) {
					String[] vs=value.split(";;");
					
					for(int i=0;i<vs.length;i++) {
						fs=vs[i].split("\t");
						if (value.startsWith("docs\t"))
							listA.add(new Text(fs[1]));
						if (value.startsWith("codes\t"))
							listB.add(new Text(fs[1]));
					}
					
				}else {
					fs=value.split("\t");
					
					if (value.startsWith("docs\t"))
						listA.add(new Text(fs[1]));
					if (value.startsWith("codes\t"))
						listB.add(new Text(fs[1]));
					
				}

			}
			
			System.out.println("");
			
			for (Text A : listA)
				for (Text B : listB) {
					context.write(new Text(key.toString().split("\t")[2]), new Text(A.toString() + "\t" + B.toString()));
				}
			
		//	joinAndWrite(new Text(key.toString().split("\t")[2]), context);
		}

		private void joinAndWrite(Text key, Context context) throws IOException, InterruptedException {
			// inner join
			if (joinType.equalsIgnoreCase("inner")) {
				if (!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA)
						for (Text B : listB) {
							context.write(key, new Text(A.toString() + "\t" + B.toString()));
						}
				}
			}
			// left outer join
			if (joinType.equalsIgnoreCase("leftouter")) {
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(key, new Text(A.toString() + "\t" + B.toString()));
							}
						} else {
							context.write(key, new Text(A.toString() + "\t" + EMPTY_VALUE));
						}
					}
				}
			}
			// right outer join
			else if (joinType.equalsIgnoreCase("rightouter")) {
				if (!listB.isEmpty()) {
					for (Text B : listB) {
						if (!listA.isEmpty()) {
							for (Text A : listA)
								context.write(key, new Text(A.toString() + "\t" + B.toString()));
						} else {

							context.write(key, new Text(EMPTY_VALUE + "\t" + B.toString()));
						}
					}
				}
			}
			// full outer join
			else if (joinType.equalsIgnoreCase("fullouter")) {
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(key, new Text(A.toString() + "\t" + B.toString()));
							}
						} else {
							context.write(key, new Text(A.toString() + "\t" + EMPTY_VALUE));
						}
					}
				} else {
					for (Text B : listB)
						context.write(key, new Text(EMPTY_VALUE + "\t" + B.toString()));
				}
			}
			// anti join
			else if (joinType.equalsIgnoreCase("anti")) {
				if (listA.isEmpty() ^ listB.isEmpty()) {
					for (Text A : listA)
						context.write(key, new Text(A.toString() + "\t" + EMPTY_VALUE));
					for (Text B : listB)
						context.write(key, new Text(EMPTY_VALUE + "\t" + B.toString()));
				}
			}
		}
	}
	
	public static int size(Iterable data) {
		 
	    if (data instanceof Collection) {
	        return ((Collection<?>) data).size();
	    }
	    int counter = 0;
	    for (Object i : data) {
	        counter++;
	    }
	    return counter;
	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
	 
	    /**
	     * Combiner的作用：主要为了合并数据，执行在map
	     * -partitioner之后，reduce之前。使用之后传输到reduce的数据量有所减少才是Combiner存在的意义
	     * 
	     */
	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException {
	    	
	    	String vs="";
	    	boolean has_value=false;
	    	for(Text t: values) {
	    		has_value=true;
	    		vs+=t.toString()+";;";
	    	}
	    	vs=vs.trim();
	    	if(vs.endsWith(";;")) {
	    		vs=vs.substring(0,vs.length()-2);
	    	}
	    	
	    	if(has_value)
	    		context.write(key, new Text(vs));

	    }
	}
	
	public static String getIP(String hdfs_url) {
		String[] fs=hdfs_url.split(":");
		return fs[1].replace("/","");
	}
	
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "PJoin-CombinerMN";
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// default parameter
		if (args.length == 0) { 

			args = new String[] { 
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/input-smalldata", // allow multiple input
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/input-data",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/output-pjoin-combiner", 
					"@jointype=inner",
					"@traceId=" + System.currentTimeMillis(),
					"@jobName=" + jobName,
					"@hasHeader"+"=false",
					"@m=3",
					"@n=3"
					
			};	
		}
		
		//obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);
		
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");
		
		if(!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, jobName);
		
		int numberOfReducers=1;
		
		if(appArgs.containsKey("reducerNum")) {
			numberOfReducers=Integer.parseInt(appArgs.get("reducerNum"));
		}
		
	 

		conf.set("mapred.jop.tracker", "hdfs://"+getIP(args[0])+":9001");
		conf.set("fs.defaultFS", "hdfs://"+getIP(args[0])+":9000");
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[2]), true);
		fs.delete(new Path(args[2]+"-counter"), true);
		
		// 行数job的配置
		Job counterJob = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Counter]");
		
		counterJob.setJarByClass(PartitionJoin.class);

		counterJob.setMapperClass(LCMapper.class);
		
		counterJob.setOutputKeyClass(Text.class);
		counterJob.setOutputValueClass(Text.class);
	//	counterJon.setReducerClass(DocReducer.class);
 

		//FileInputFormat.addInputPaths(counterJob, args[0]+";"+args[1]);
		
		MultipleInputs.addInputPath(counterJob,
                new Path(args[0]),TextInputFormat.class);
		MultipleInputs.addInputPath(counterJob,
                new Path(args[1]),TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(counterJob, new Path(args[2]+"-counter"));

		counterJob.waitForCompletion(true);
 
		fs.delete(new Path(args[2]+"-counter"), true);
		
		long ln=counterJob.getCounters().findCounter(LineCounters.L_LINE_COUNT).getValue();
		long rn=counterJob.getCounters().findCounter(LineCounters.R_LINE_COUNT).getValue();
		
		String hasHeader="true";
		
		if(appArgs.containsKey("hasHeader")) {
			hasHeader=appArgs.get("hasHeader");
		}
		
		if(hasHeader.equals("true")) {
			ln=ln-1;
			rn=rn-1;
		}
			
		int m=Integer.parseInt(appArgs.get("m"));
		int n=Integer.parseInt(appArgs.get("n"));
		
		int L_Block=0;
		int R_Block=0;
		int N=0;
		int M=0;
		
		if(ln%n>0)
			L_Block=(int)(ln*1.0/m)+1;
		else
			L_Block=(int)(ln*1.0/m);
		
		if(rn%n>0)
			R_Block=(int)(rn*1.0/n)+1;
		else
			R_Block=(int)(rn*1.0/n);
		
		N=n;
		M=m;
		

		System.out.println("Config: "+L_Block+","+R_Block+","+N);
 
		//delete output

		conf.set("L_Block", L_Block+"");
		conf.set("R_Block", R_Block+"");
		conf.set("N", N+"");
		conf.set("M", M+"");

		//set job
		Job job = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Join-END]");
		job.setJarByClass(PJCombiner2.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if(appArgs.containsKey("reducerNum")) {
			job.setNumReduceTasks(numberOfReducers);
		}
		
		

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CodeJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DocJoinMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.getConfiguration().set("join.type", appArgs.get("jointype"));

		//time cost
		long startTime = System.currentTimeMillis(); 

		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis(); 
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
	}

}
