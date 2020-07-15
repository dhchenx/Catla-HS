package cn.edu.bjtu.cdh.bigdata.research.icd11.coding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

public class CAC_Cosine {

	public static class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {

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
		
		public String segWordToStr(String content, boolean useSmart) {
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
				return sb.toString();
			} catch (Exception ex) {
				return content;
			}
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

			String fileName = getFileName(context);

			if (value.toString().isEmpty())
				return;

			String[] ls = value.toString().split("\t");

			//String id = ls[0];
			String id = "";

			if (ls.length > 1) {

				String type = "";
				if (fileName.startsWith("codes")) {
			 
					Counter codeCounter = context.getCounter("Codes",
							"id");
					codeCounter.increment(1L);
					type = "Code";
					id="C"+codeCounter.getValue();
					context.write(new Text("code"), new Text(id));
					context.write(new Text("code_line"), new Text(id+"\t"+ segWordToStr(ls[1],false)+ "\t"+ls[0]));
					
				} else if (fileName.startsWith("docs")) {
					 
					Counter codeCounter = context.getCounter("Docs",
							"id");
					codeCounter.increment(1L);
					id="D"+codeCounter.getValue();
					type = "Disease";
					context.write(new Text("doc"), new Text(id));
					
					context.write(new Text("doc_line"), new Text(id+"\t"+segWordToStr(ls[1],false) + "\t"+ls[0]));
					
				}

			}
			

		}

	}

	public static class CounterReducer extends Reducer<Text, Text, Text, Text> {

		private MultipleOutputs<Text, Text> mos = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 创建多文件输出对象
			mos = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> vs = new ArrayList<String>();

			for (Text t : values) {
				String s = t.toString();
				vs.add(s);
			}

			if (key.toString().equals("doc")) {

				mos.write("doccounter", new Text("" + vs.size()), new Text(""));

			}
			else
			if (key.toString().equals("code")) {
				mos.write("codecounter", new Text("" + vs.size()), new Text(""));

			}
			
			if (key.toString().equals("doc_line")) {
				
				for(int i=0;i<vs.size();i++)
					mos.write("docs", new Text(vs.get(i)),new Text());

			}else if(key.toString().equals("code_line")) {
				for(int i=0;i<vs.size();i++)
					mos.write("codes", new Text(vs.get(i)),new Text());
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// 关闭多文件输出对象，刷新缓存数据
			mos.close();
		}
	}

	public static class IndexingMapper extends Mapper<LongWritable, Text, Text, Text> {

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
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String fileName = getFileName(context);
			
			if (value.toString().isEmpty())
				return;
			
			if(!(fileName.startsWith("docs-")||fileName.startsWith("codes-"))) {
				return;
			}

			String[] ls = value.toString().trim().split("\t");

			String id = ls[0];
			
			ls=ls[1].split(" ");

			if (ls.length > 1) {

				List<String> unique_words = new ArrayList<String>();
				List<Integer> unique_word_counts = new ArrayList<Integer>();
				for (int i = 0; i < ls.length; i++) {
					if (unique_words.contains(ls[i])) {
						int index = unique_words.indexOf(ls[i]);
						unique_word_counts.set(index, unique_word_counts.get(index) + 1);

					} else {
						unique_words.add(ls[i]);
						unique_word_counts.add(1);
					}

				}

				String type = "";
			 
				if (fileName.startsWith("codes-")) {
					type = "Code";
				 

				} else if (fileName.startsWith("docs-")) {
				 
					type = "Disease";

				}

				for (int i = 0; i < unique_words.size(); i++) {
					context.write(new Text(unique_words.get(i)),
							new Text(type + "\t" + id + "\t" + unique_word_counts.get(i)));
				}
			}

		}
	}

	public static class IndexingReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> vs = new ArrayList<String>();

			for (Text t : values) {
				String s = t.toString();
				vs.add(s);
			}

			String ks = "";

			List<String> dlist = new ArrayList<String>();
			List<String> tlist = new ArrayList<String>();
			List<Integer> flist = new ArrayList<Integer>();

			for (int i = 0; i < vs.size(); i++) {
				String[] ls = vs.get(i).split("\t");

				dlist.add(ls[1]);
				tlist.add(ls[0]);
				flist.add(Integer.parseInt(ls[2]));

			}

			// 需要改进

			long doc_counter = Long.parseLong(context.getConfiguration().get("doccounter"));
			long code_counter = Long.parseLong(context.getConfiguration().get("codecounter"));

			//若该单词不存在文档，则补全文档列表
			for (int i = 1; i <= doc_counter; i++) {
				if (!dlist.contains(("D" + i))) {
					dlist.add("D" + i);
					flist.add(0);
				}
			}

			//若该单词不存在文档，则补全文档列表
			for (int i = 1; i <= code_counter; i++) {
				if (!dlist.contains(("C" + i))) {
					dlist.add("C" + i);
					flist.add(0);
				}
			}

			for (int i = 0; i < dlist.size(); i++) {

				if (i != dlist.size() - 1) {
					ks += dlist.get(i) + "," + flist.get(i) + ";";
				} else {
					ks += dlist.get(i) + "," + flist.get(i);
				}

			}

			if (!ks.equals(""))
				context.write(key, new Text(ks));

		}

	}

	public static class PairwiseSimMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println(value.toString());
			String word = value.toString().split("\t")[0];

			String[] doc_freq_list = value.toString().split("\t")[1].split(";");

			List<String> doc_list = new ArrayList<String>();
			List<Integer> docf_list = new ArrayList<Integer>();
			List<String> code_list = new ArrayList<String>();
			List<Integer> codef_list = new ArrayList<Integer>();

			for (int i = 0; i < doc_freq_list.length; i++) {
				String d = doc_freq_list[i].split(",")[0];

				int f = Integer.parseInt(doc_freq_list[i].split(",")[1]);

				if (d.startsWith("D")) {
					doc_list.add(d);
					docf_list.add(f);
				} else if (d.startsWith("C")) {
					code_list.add(d);
					codef_list.add(f);
				}
			}

			for (int i = 0; i < doc_list.size(); i++) {
				for (int j = 0; j < code_list.size(); j++) {
					if(!(docf_list.get(i)==0&&codef_list.get(j)==0)) {
						
						context.write(new Text(doc_list.get(i) + "," + code_list.get(j)),
								new Text(word + "\t" + docf_list.get(i) + "\t" + codef_list.get(j)));

					}
				
				}
			}

		}
	}

	public static class PairwiseSimReducer extends Reducer<Text, Text, Text, Text> {

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

		public String getListStr(List<String> t) {
			String s = "";
			for (int i = 0; i < t.size(); i++) {
				if (i != t.size() - 1) {
					s += t.get(i) + ",";
				} else {
					s += t.get(i);
				}
			}
			return s;
		}

		public String getDoubleListStr(List<Double> t) {
			String s = "[";
			for (int i = 0; i < t.size(); i++) {
				if (i != t.size() - 1) {
					s += (int) ((double) t.get(i)) + ",";
				} else {
					s += (int) ((double) t.get(i));
				}
			}
			s += "]";
			return s;
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> vs = new ArrayList<String>();

			for (Text t : values) {
				String s = t.toString();
				vs.add(s);
			}

			List<Double> v1 = new ArrayList<Double>();
			List<Double> v2 = new ArrayList<Double>();
			List<String> ws = new ArrayList<String>();

			System.out.println("-->" + key.toString());
			for (int i = 0; i < vs.size(); i++) {
				String[] ls = vs.get(i).split("\t");

				double i1 = Integer.parseInt(ls[1]);
				double i2 = Integer.parseInt(ls[2]);
				//if (!(i1 == 0 && i2 == 0)) {
					v1.add(i1);
					v2.add(i2);
					ws.add(ls[0]);
				//}

				System.out.println(vs.get(i));
			}

			double[] d1 = new double[v1.size()];
			double[] d2 = new double[v1.size()];
			for (int i = 0; i < v1.size(); i++) {
				d1[i] = v1.get(i);
				d2[i] = v2.get(i);
			}

			double sim = cosineSimilarity(d1, d2);
			
			if(sim>=0.2)
			context.write(key,
					new Text(sim+""));

		}

	}

	public static String getCounter(Configuration conf, String path) {
		try {
			FileSystem fs_counter = FileSystem.get(conf);
			Path file = new Path( path);
			FSDataInputStream inStream = fs_counter.open(file);
			BufferedReader d = new BufferedReader(new InputStreamReader(inStream));
			String ss = "";
			String counter = "";
			while ((ss = d.readLine()) != null) {

				counter = ss;
			}
			d.close();
			fs_counter.close();
			return counter;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return "";
	}
	
	public static String getIP(String hdfs_url) {
		String[] fs=hdfs_url.split(":");
		return fs[1].replace("/","");
	}


	public static void main(String[] args) throws Exception {


		
		if (args.length == 0) {
			args = new String[] { 
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/input-100",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/output_counter",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/output_index",
					"hdfs://192.168.159.132:9000/data/cdh/test/icd-simcac-value/output_similarity"
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

		// counter job，统计所有文档的数量

		Job counterJob = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Count1]");
		counterJob.setJarByClass(CAC_Cosine.class);

		counterJob.setMapperClass(CounterMapper.class);

		MultipleOutputs.addNamedOutput(counterJob, "doccounter", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(counterJob, "codecounter", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(counterJob, "codes", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(counterJob, "docs", TextOutputFormat.class, Text.class, Text.class);
		
		counterJob.setReducerClass(CounterReducer.class);

		counterJob.setOutputKeyClass(Text.class);
		counterJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(counterJob, new Path(args[0]));

		FileOutputFormat.setOutputPath(counterJob, new Path(args[1]));

		// indexing job 对所有文档的单词进行倒排索引

		Job indexJob = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Indexing2]");
		indexJob.setJarByClass(CAC_Cosine.class);

		indexJob.setMapperClass(IndexingMapper.class);

		indexJob.setReducerClass(IndexingReducer.class);

		indexJob.setOutputKeyClass(Text.class);
		indexJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(indexJob, new Path(args[1]));
	 
		FileOutputFormat.setOutputPath(indexJob, new Path(args[2]));

		// similarity job 计算两两文档之间的相似度，目前使用the cosine similarity measure

		Job simJob = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "-Similarity3-END]");
		simJob.setJarByClass(CAC_Cosine.class);

		FileInputFormat.setInputPaths(simJob, new Path(args[2] + "/part-*"));

		simJob.setMapperClass(PairwiseSimMapper.class);

		simJob.setMapOutputKeyClass(Text.class);
		simJob.setMapOutputValueClass(Text.class);

		simJob.setReducerClass(PairwiseSimReducer.class);
		FileOutputFormat.setOutputPath(simJob, new Path(args[3]));
		simJob.setOutputKeyClass(Text.class);
		simJob.setOutputValueClass(Text.class);

		long startTime = System.currentTimeMillis(); // 获取开始时间

		
		// 提交job1及job2,并等待完成
		if (counterJob.waitForCompletion(true)) {

			String doc_counter = getCounter(conf, args[1]+ "/doccounter-r-00000");
			String code_counter = getCounter(conf, args[1]+ "/codecounter-r-00000");

			indexJob.getConfiguration().set("codecounter", code_counter.trim());
			indexJob.getConfiguration().set("doccounter", doc_counter.trim());

			if (indexJob.waitForCompletion(true)) {
				if (simJob.waitForCompletion(true)) {
					System.out.println("finished!");
					long endTime = System.currentTimeMillis(); // 获取结束时间
					System.out.println("程序运行时间(success)： " + (endTime - startTime) + "ms");
				}
			}
		}

	}
}