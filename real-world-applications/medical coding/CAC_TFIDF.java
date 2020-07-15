package cn.edu.bjtu.cdh.bigdata.research.icd11.coding;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//for coding

public class CAC_TFIDF {
	
// 参考资料 https://link.springer.com/content/pdf/10.1007%2F978-3-662-44722-2.pdf

	
	// user map
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
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
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String fileName=getFileName(context);
			String prefix="";
			if(fileName.startsWith("docs"))
				prefix="docs-";
			if(fileName.startsWith("codes"))
				prefix="codes-";
			
			
			
			String line = value.toString();
			String[] items = line.split("\t");
			String docId = prefix+ items[0];
			
			if(items==null||items.length<2)
				return;
			
			items=segWords(items[1],false);
			 
			for (int i = 1; i < items.length; i++) {
				//System.out.println(items[i]);
				context.write(new Text(items[i] + "\t" + docId), new Text(1+""));
			}
			 
		}
	}

	public static class WordCountReducer extends Reducer<Text,Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Iterator<Text> iterator = values.iterator();
			int sum = 0;
			while (iterator.hasNext()) {
				String value=iterator.next().toString();
				sum++;
			}
			 
			context.write(key, new Text(sum+""));

		}

	}

	// user map
	public static class TermFreqMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] items = line.split("\t");
			String term = items[0];
			String docId = items[1];
			int sum = Integer.parseInt(items[2]);

			context.write(new Text(docId), new Text(term + "\t" + sum));
		}
	}

	public static class TermFreqReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Iterator<Text> iterator = values.iterator();
			int N = 0;
			List<String> vs = new ArrayList<String>();
			while (iterator.hasNext()) {
				String term_o = iterator.next().toString();
				N = N + Integer.parseInt(term_o.split("\t")[1]);
				vs.add(term_o);
			}

			for (int i = 0; i < vs.size(); i++)
				context.write(new Text(key.toString() + "\t" + N), new Text(vs.get(i)));

		}

	}

	public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] items = line.split("\t");
			String docId = items[0];
			String term = items[2];
			String N = items[1];
			String o = items[3];

			context.write(new Text(term), new Text(docId + "\t" + o + "\t" + N));
		}
	}

	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String term = key.toString();
			int n = 0;

			Iterator<Text> iterator = values.iterator();

			List<String> vs = new ArrayList<String>();
			while (iterator.hasNext()) {
				String docId_o_N = iterator.next().toString();
				n = n + 1;
				vs.add(docId_o_N);
			}

			for (int i = 0; i < vs.size(); i++) {
				String[] ls = vs.get(i).split("\t");
				String docId = ls[0];
				int o = Integer.parseInt(ls[1]);
				int N = Integer.parseInt(ls[2]);
				double tf = o * 1.0 / N;
				int D = 8;
				double idf = Math.log10(Math.abs(D) * 1.0 / (1 + n));
				context.write(new Text(docId), new Text(term + "\t" + tf * idf));
			}

		}

	}
	
	public static class CosineMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Map<String, List<String>> docTermMap = new HashedMap();
		private Map<String, List<String>> codeTermMap = new HashedMap();
		private List<String> docIds=new ArrayList<String>();
		private List<String> codeIds=new ArrayList<String>();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] items = line.split("\t");
			String docId=items[0];
			String term=items[1];
			String tf_idf=items[2];
			
			if(docId.startsWith("docs-")) {
			if(docTermMap.containsKey(docId)) {
				docTermMap.get(docId).add(term+"\t"+tf_idf);
			}else {
				List temp=new ArrayList<String>();
				temp.add(term+"\t"+tf_idf);
				docTermMap.put(docId, temp);
				docIds.add(docId);
			}
			}
			
			if(docId.startsWith("codes-")) {
			if(codeTermMap.containsKey(docId)) {
				codeTermMap.get(docId).add(term+"\t"+tf_idf);
			}else {
				List temp=new ArrayList<String>();
				temp.add(term+"\t"+tf_idf);
				codeTermMap.put(docId, temp);
				codeIds.add(docId);
			}
			}
			
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (int m=0;m<docIds.size();m++) {
				for (int n=0;n<codeIds.size();n++) {
					
					List<String> A=docTermMap.get(docIds.get(m));
					List<String> B=codeTermMap.get(codeIds.get(n));
					
					String si="";
					String siv="";
					for(int i=0;i<A.size();i++)
					{
						String[] v=A.get(i).split("\t");
						
						if(i!=A.size()-1) {
							si+=v[0]+" ";
							siv+=v[1]+" ";
						}
						else {
							si+=v[0];
							siv+=v[1];
						}
					}
					
					String sj="";
					String sjv="";
					for(int i=0;i<B.size();i++)
					{
						String[] v=B.get(i).split("\t");
						
						if(i!=B.size()-1) {
							sj+=v[0]+" ";
							sjv+=v[1]+" ";
						}
						else {
							sj+=v[0];
							sjv+=v[1];
						}
					}
					//System.out.println(si+"\t"+sj+"\t"+siv+"\t"+sjv);
					context.write(new Text(docIds.get(m).replace("docs-", "")+"\t"+codeIds.get(n).replace("codes-", "")), new Text(si+"\t"+sj+"\t"+siv+"\t"+sjv));
				}
			}
		 
		}
	}
	
	public static class CosineReducer extends Reducer<Text, Text, Text, Text> {

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
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			

			Iterator<Text> iterator = values.iterator();

			List<String> vs = new ArrayList<String>();
			while (iterator.hasNext()) {
				String value = iterator.next().toString();
				String[] fs=value.split("\t");
				
				String[] A=fs[0].split(" ");
				String[] B=fs[1].split(" ");
				
				String[] A_tfidf=fs[2].split(" ");
				String[] B_tfidf=fs[3].split(" ");
				
				double[] dA=new double[A_tfidf.length];
				double[] dB=new double[B_tfidf.length];
				
				for(int i=0;i<A_tfidf.length;i++) {
					dA[i]=Double.parseDouble(A_tfidf[i]);
				}
				
				
				for(int i=0;i<B_tfidf.length;i++) {
					dB[i]=Double.parseDouble(B_tfidf[i]);
				}
				
				List<String> all_terms=new ArrayList<String>();
				List<String> A_terms=new ArrayList<String>();
				List<String> B_terms=new ArrayList<String>();
				for(String s:A) {
					if(!all_terms.contains(s))
						all_terms.add(s);
					A_terms.add(s);
				}
				for(String s:B) {
					if(!all_terms.contains(s))
						all_terms.add(s);
					B_terms.add(s);
				}
				double[] v1=new double[all_terms.size()];
				for(int i=0;i<all_terms.size();i++) {
					if(A_terms.contains(all_terms.get(i)))
					{
						int index=A_terms.indexOf(all_terms.get(i));
						v1[i]=dA[index];
					}else {
						v1[i]=0;
					}
				}
				
				double[] v2=new double[all_terms.size()];
				for(int i=0;i<all_terms.size();i++) {
					if(B_terms.contains(all_terms.get(i)))
					{
						int index=B_terms.indexOf(all_terms.get(i));
						v2[i]=dB[index];
					}else {
						v2[i]=0;
					}
				}
				
				double cos_sim=cosineSimilarity(v1,v2);
				
				if(cos_sim>=0.2)
					context.write(key, new Text(""+cos_sim));
				
				
			}

		

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
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/input-100", // allow multiple input
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value-10000/output-cosine-wordcount",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value-10000/output-cosine-termfreq",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value-10000/output-cosine-tdidf",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value-10000/output-cosine-sim"
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
			appArgs.put(InjectVars.jobName, "CAC_Cosine");

		// define the job name

		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		fs.delete(new Path(args[2]), true);
		fs.delete(new Path(args[3]), true);
		fs.delete(new Path(args[4]), true);

		// 第一个job的配置
		Job job1 = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-WordCount1]");
		job1.setJarByClass(CAC_TFIDF.class);

		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// 第二个job的配置
		Job job2 = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-TermFreq2]");
		job2.setJarByClass(CAC_TFIDF.class);

		job2.setMapperClass(TermFreqMapper.class);
		job2.setReducerClass(TermFreqReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		

		// 第三个job的配置
		Job job3 =  Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-TFIDF3]");
		job3.setJarByClass(CAC_TFIDF.class);

		job3.setMapperClass(TFIDFMapper.class);
		job3.setReducerClass(TFIDFReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		
		// 第四个job的配置
		Job job4 =  Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Cosine4-END]");
		job4.setJarByClass(CAC_TFIDF.class);

		job4.setMapperClass(CosineMapper.class);
		job4.setReducerClass(CosineReducer.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job4, new Path(args[3]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));

		// 加入控制容器
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);

		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);

		ControlledJob ctrljob3 = new ControlledJob(conf);
		ctrljob3.setJob(job3);

		ControlledJob ctrljob4 = new ControlledJob(conf);
		ctrljob4.setJob(job4);

		// 意思为job2的启动，依赖于job1作业的完成
		ctrljob2.addDependingJob(ctrljob1);
		ctrljob3.addDependingJob(ctrljob2);
		ctrljob4.addDependingJob(ctrljob3);
		
		// 主的控制容器，控制上面的总的两个子作业
		JobControl jc = new JobControl("myctrl");
		jc.addJob(ctrljob1);
		jc.addJob(ctrljob2);
		jc.addJob(ctrljob3);
		jc.addJob(ctrljob4);

		// 在线程启动，记住一定要有这个
		Thread t = new Thread(jc);
		t.start();

		long startTime = System.currentTimeMillis(); // 获取开始时间

		 Thread jcThread = new Thread(jc);  
	        jcThread.start();  
	        while(true){  
	            if(jc.allFinished()){  
	                System.out.println(jc.getSuccessfulJobList());  
	                jc.stop();  
	                long endTime = System.currentTimeMillis(); // 获取结束时间
	        		System.out.println("程序运行时间(success)： " + (endTime - startTime) + "ms");
	        		break;
	            }  
	            if(jc.getFailedJobList().size() > 0){  
	                System.out.println(jc.getFailedJobList());  
	                jc.stop();  
	                long endTime = System.currentTimeMillis(); // 获取结束时间
	        		System.out.println("程序运行时间：(failed) " + (endTime - startTime) + "ms");
	        		break;
	            }  
	        } 


	}

}
