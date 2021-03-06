package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PartitionJoin2 {

	static enum PartitionNums {
		L_NUM,
		R_NUM,
		L_Partition_NUM,
		R_Partition_NUM
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
			context.getCounter(PartitionNums.R_NUM).increment(1);
			
			//获取当前行数
			long r_num=context.getCounter(PartitionNums.R_NUM).getValue();
			
			//计算当前的第几个分区
			long current_p_num=(long)((r_num-1)/R_Block);
			
			//更新全局L当前第几个分区
		//	context.getCounter(PartitionNums.R_Partition_NUM).setValue(current_p_num);
			
			System.out.println("L{i}\t"+"R"+current_p_num);
			System.out.println("R_NUM="+r_num+", R_BLOCK="+R_Block);
			System.out.println("N="+N);
			
			for(int i=0;i<N;i++) {
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
				String[] fs=value.split("\t");
				System.out.println(value);
				if (value.startsWith("docs\t"))
					listA.add(new Text(fs[1]));
				if (value.startsWith("codes\t"))
					listB.add(new Text(fs[1]));
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
	
	public static String getIP(String hdfs_url) {
		String[] fs=hdfs_url.split(":");
		return fs[1].replace("/","");
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "PartitionJoin2";
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// default parameter
		if (args.length == 0) { 

			args = new String[] { 
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/input-smalldata", // allow multiple input
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/input-data",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-test/output-partitionjoin2", 
					"@jointype=inner",
					"@traceId=" + System.currentTimeMillis(),
					"@jobName=" + jobName,
					"@ln=5",
					"@rn=4",
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
		
		int ln=Integer.parseInt(appArgs.get("ln"));
		int rn=Integer.parseInt(appArgs.get("rn"));
		int n=Integer.parseInt(appArgs.get("n"));
		
		int L_Block=0;
		int R_Block=0;
		int N=0;
		
		if(ln%n>0)
			L_Block=(int)(ln*1.0/n)+1;
		else
			L_Block=(int)(ln*1.0/n);
		
		if(rn%n>0)
			R_Block=(int)(rn*1.0/n)+1;
		else
			R_Block=(int)(rn*1.0/n);
		
		N=n;
		
	 
		
		System.out.println("Config: "+L_Block+","+R_Block+","+N);
 
		//delete output
	 
		conf.set("mapred.jop.tracker", "hdfs://"+getIP(args[0])+":9001");
		conf.set("fs.defaultFS", "hdfs://"+getIP(args[0])+":9000");
		
		conf.set("L_Block", L_Block+"");
		conf.set("R_Block", R_Block+"");
		conf.set("N", N+"");
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[2]), true);

		//set job
		Job job = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");
		job.setJarByClass(PartitionJoin2.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
