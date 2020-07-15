package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

/*
 * һ�����һ��С��
 * map �׶Σ�BloomFilter ���С���key�������ڴ�����Ȼ��Ų��µĳ��������˴��
 * reduce �׶Σ�reduce side join 
 */

public class BloomFilteringJoin {
	/**
	 * Ϊ���Բ�ͬ��(�ļ�)��key/value�Դ��ǩ������ͬ��Դ�ļ�¼�� Ȼ���������ֶ���Ϊkey�����ಿ�ֺ��¼ӵı�־��Ϊvalue�������������
	 */
	public static class BloomFilteringMapper extends Mapper<Object, Text, Text, Text> {
		// ��һ��������vector�Ĵ�С�����ֵ�������Ĵ󣬿��Ա���hash�����ʱ����������ظ�
		// �ڶ���������ɢ�к����ĸ���
		// ��������hash�����ͣ���Ȼ��int�ͣ�����ֻ��Ĭ������ֵ
		// ��ϣ��������k��λ�����Сm���ַ�������n֮������໥��ϵ
		// n ΪС���¼��,��������Ĵ�����E������ȷ�����ʵ�λ�����С����m >= log2(e) * (n * log2(1/E))
		// ����m��n������ȷ������hash��������k = ln2 * (m/n)����ʱ��������С
		private BloomFilter filter = new BloomFilter(100*10000, 12, Hash.MURMUR_HASH);
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
					String[] records = StringUtils.split(infoAddr.toString(), "\t");
					if (records != null && records.length>=2) {

						filter.add(new Key(records[1].getBytes()));
						
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
			// System.out.println(pathName);
			// �������������records����һ��records�ı��
			if (pathName.startsWith("docs")) {
				String[] valueItems = value.toString().split("\t");
				// ���˵�������
				if (valueItems.length< 2) {
					return;
				}
				// ͨ��filter ���˴���е�����
				if (filter.membershipTest(new Key(valueItems[1].getBytes()))) {
					joinKey.set(valueItems[1]);
					combineValue.set("docs-" + valueItems[0]);
					System.out.println("docs\t" + joinKey.toString() + "\t" + combineValue.toString());
					context.write(joinKey, combineValue);
				} else {
					// System.out.println("nomapping: "+value.toString());
				}

			} else if (pathName.startsWith("codes")) {
				// �������������station����һ��station�ı��
				String[] valueItems = value.toString().split("\t");
				// ���˵�������
				if (valueItems.length< 2) {
				 return;
				}
				joinKey.set(valueItems[1]);
				combineValue.set("codes-" + valueItems[0]);
				// System.out.println("codes\t"+joinKey.toString()+"\t"+combineValue.toString());
				context.write(joinKey, combineValue);
			}

		}
	}

	/*
	 * reduce �����ѿ�����
	 */
	public static class BloomFilteringReducer extends Reducer<Text, Text, Text, Text> {
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
		String[] fs = hdfs_url.split(":");
		return fs[1].replace("/", "");
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		String jobName = "BloomFilteringJoin";
		if (args.length == 0) {
		 
			
		
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-smalldata/codes.txt", // small
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input", 
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/output-bloomfilter" ,
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

		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");

		// ���·��
		Path mypath = new Path(args[args.length - 1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// �������·��
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");

		// ��ӻ����ļ�
		job.addCacheFile(new URI(args[0]));

		job.setJarByClass(BloomFilteringJoin.class);
		job.setMapperClass(BloomFilteringMapper.class);
		job.setReducerClass(BloomFilteringReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// ��������ļ�
		for (int i = 1; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		// �������·��
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		// α����
		long startTime = System.currentTimeMillis(); // ��ȡ��ʼʱ��

		job.waitForCompletion(true);

		long endTime = System.currentTimeMillis(); // ��ȡ����ʱ��
		System.out.println("��������ʱ�䣺 " + (endTime - startTime) + "ms");

	}
}