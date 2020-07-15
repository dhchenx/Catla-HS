package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.Key;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

/*
 * һ�����һ��С��Ҳ�ܴ��ڴ��зŲ��£�
 * map �׶Σ�Semi Join���С��������¼�ڴ�Ų��µĳ�������ô��ȡ����һС���ֹؼ��ֶη����ڴ棬���˴��
 * ��ǰ���ˣ���ǰ��ȡ��С���е������ֶη����ڴ��У���map�׶ξͽ����´������ЩС���д��ڵ������ֶ�key
 * reduce �׶Σ�reduce side join
 */
public class SemiReduceJoin {
	/**
	 * Ϊ���Բ�ͬ��(�ļ�)��key/value�Դ��ǩ������ͬ��Դ�ļ�¼�� Ȼ���������ֶ���Ϊkey�����ಿ�ֺ��¼ӵı�־��Ϊvalue�������������
	 */
	public static class SemiJoinMapper extends Mapper<Object, Text, Text, Text> {
		// ����Set���ϱ���С���е�key
		private Set<String> joinKeys = new HashSet<String>();
		private Text joinKey = new Text();
		private Text combineValue = new Text();

		/**
		 * ��ȡ�ֲ�ʽ�����ļ�
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader br;
			String infoAddr = null;
			// ���ػ����ļ�·��

			URI[] cacheFilesPaths = context.getCacheFiles();
			for (URI path : cacheFilesPaths) {
				String pathStr = path.getPath();
				// System.out.println(pathStr);
				FileSystem fs = FileSystem.get(context.getConfiguration());

				br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathStr)), "UTF-8"));
				while (null != (infoAddr = br.readLine())) {
					// ���ж�ȡ����������վ����
					// System.out.println(infoAddr.toString());
					String[] records = StringUtils.split(infoAddr.toString(), "\t");
					if (records != null && !records[0].contains("_ID")&&records.length>=2) {
						if (true) {
							System.out.println(records[1]);
							joinKeys.add(records[1].trim());
						}
					}
				}
			}
		}

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
			String pathName = getFileName(context);
			// �������������records����һ��records�ı��
			if (pathName.startsWith("docs")) {
				String[] valueItems = StringUtils.split(value.toString(), "\t");
				
				if(value.toString().isEmpty()||valueItems[0].contains("_ID"))
					return;
				
				if(valueItems.length<2)
					return;

				if (joinKeys.contains(valueItems[1].trim())) {
					joinKey.set(valueItems[1]);
					combineValue.set("docs-" + valueItems[0]);

					context.write(joinKey, combineValue);
				}

			} else if (pathName.startsWith("codes")) {
				 
				String[] valueItems = StringUtils.split(value.toString(), "\t");
				
				if(value.toString().isEmpty()||valueItems[0].contains("_ID"))
					return;
				
				if(valueItems.length<2)
					return;
				
				joinKey.set(valueItems[1]);
				combineValue.set("codes-" + valueItems[0]);
				
				context.write(joinKey, combineValue);

			}
		}
	}

	/*
	 * reduce �����ѿ�����
	 */
	public static class SemiJoinReducer extends Reducer<Text, Text, Text, Text> {
		private List<String> leftTable = new ArrayList<String>();
		private List<String> rightTable = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// һ��Ҫ�������
			leftTable.clear();
			rightTable.clear();
			// ��ͬkey�ļ�¼����鵽һ��������Ҫ����ͬkey�������ڲ�ͬ������ݷֿ���Ȼ�����ѿ�����
			for (Text value : values) {
				String val = value.toString();

				if (val.startsWith("codes-")) {
					leftTable.add(val.replaceFirst("codes-", ""));
				} else if (val.startsWith("docs-")) {
					System.out.println("key=" + key.toString() + "\t" + ";value=" + value.toString());
					rightTable.add(val.replaceFirst("docs-", ""));
				}
			}
			// �ѿ�����
			for (String leftPart : leftTable) {
				for (String rightPart : rightTable) {
					result.set(leftPart + "\t" + rightPart);
					context.write(key, result);
				}
			}
		}
	}
	
	public static String getIP(String hdfs_url) {
		String[] fs=hdfs_url.split(":");
		return fs[1].replace("/","");
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		String jobName = "SemiReduceJoin";
		if (args.length == 0) {
			
	
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-smalldata/codes.txt", // small	
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/output-semireducejoin",
							"@traceId=" + System.currentTimeMillis(),
							"@jobName=" + jobName
			};
		}
		
		//obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);
		// set default if not exists @vars
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");

		if (!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, jobName);
		//config
		

		conf.set("mapred.jop.tracker", "hdfs://"+getIP(args[0])+":9001");
		conf.set("fs.defaultFS", "hdfs://"+getIP(args[0])+":9000");
		
		//delete
		Path mypath = new Path(args[args.length - 1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// �������·��
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		
		Job job = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");

		// add cache file
		job.addCacheFile(new URI(args[0]));

		job.setJarByClass(SemiReduceJoin.class);
		job.setMapperClass(SemiJoinMapper.class);
		job.setReducerClass(SemiJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// input
		for (int i = 1; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		
		// output
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

		//time cost
		long startTime = System.currentTimeMillis(); // ��ȡ��ʼʱ��

		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis(); // ��ȡ����ʱ��
		System.out.println("��������ʱ�䣺 " + (endTime - startTime) + "ms");
	}
}