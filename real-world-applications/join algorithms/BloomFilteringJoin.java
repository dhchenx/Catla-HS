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
 * 一个大表，一个小表
 * map 阶段：BloomFilter 解决小表的key集合在内存中仍然存放不下的场景，过滤大表
 * reduce 阶段：reduce side join 
 */

public class BloomFilteringJoin {
	/**
	 * 为来自不同表(文件)的key/value对打标签以区别不同来源的记录。 然后用连接字段作为key，其余部分和新加的标志作为value，最后进行输出。
	 */
	public static class BloomFilteringMapper extends Mapper<Object, Text, Text, Text> {
		// 第一个参数是vector的大小，这个值尽量给的大，可以避免hash对象的时候出现索引重复
		// 第二个参数是散列函数的个数
		// 第三个是hash的类型，虽然是int型，但是只有默认两个值
		// 哈希函数个数k、位数组大小m及字符串数量n之间存在相互关系
		// n 为小表记录数,给定允许的错误率E，可以确定合适的位数组大小，即m >= log2(e) * (n * log2(1/E))
		// 给定m和n，可以确定最优hash个数，即k = ln2 * (m/n)，此时错误率最小
		private BloomFilter filter = new BloomFilter(100*10000, 12, Hash.MURMUR_HASH);
		private Text joinKey = new Text();
		private Text combineValue = new Text();

		/**
		 * 获取分布式缓存文件
		 */

		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader br;
			String infoAddr = null;
			// 返回缓存文件路径

			URI[] cacheFilesPaths = context.getCacheFiles();
			for (URI path : cacheFilesPaths) {
				String pathStr = path.getPath();
				// System.out.println(pathStr);
				FileSystem fs = FileSystem.get(context.getConfiguration());

				br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathStr)), "UTF-8"));
				while (null != (infoAddr = br.readLine())) {
					// 按行读取并解析气象站数据
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
			// 如果数据来自于records，加一个records的标记
			if (pathName.startsWith("docs")) {
				String[] valueItems = value.toString().split("\t");
				// 过滤掉脏数据
				if (valueItems.length< 2) {
					return;
				}
				// 通过filter 过滤大表中的数据
				if (filter.membershipTest(new Key(valueItems[1].getBytes()))) {
					joinKey.set(valueItems[1]);
					combineValue.set("docs-" + valueItems[0]);
					System.out.println("docs\t" + joinKey.toString() + "\t" + combineValue.toString());
					context.write(joinKey, combineValue);
				} else {
					// System.out.println("nomapping: "+value.toString());
				}

			} else if (pathName.startsWith("codes")) {
				// 如果数据来自于station，加一个station的标记
				String[] valueItems = value.toString().split("\t");
				// 过滤掉脏数据
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
	 * reduce 端做笛卡尔积
	 */
	public static class BloomFilteringReducer extends Reducer<Text, Text, Text, Text> {
		private List<String> leftTable = new ArrayList<String>();
		private List<String> rightTable = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 一定要清空数据
			leftTable.clear();
			rightTable.clear();
			// 相同key的记录会分组到一起，我们需要把相同key下来自于不同表的数据分开，然后做笛卡尔积
			for (Text value : values) {
				String val = value.toString();

				if (val.startsWith("codes-")) {
					leftTable.add(val.replaceFirst("codes-", ""));
				} else if (val.startsWith("docs-")) {
					rightTable.add(val.replaceFirst("docs-", ""));
				}
			}
			// 笛卡尔积
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

		// 输出路径
		Path mypath = new Path(args[args.length - 1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");

		// 添加缓存文件
		job.addCacheFile(new URI(args[0]));

		job.setJarByClass(BloomFilteringJoin.class);
		job.setMapperClass(BloomFilteringMapper.class);
		job.setReducerClass(BloomFilteringReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 添加输入文件
		for (int i = 1; i < args.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		// 伪代码
		long startTime = System.currentTimeMillis(); // 获取开始时间

		job.waitForCompletion(true);

		long endTime = System.currentTimeMillis(); // 获取结束时间
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");

	}
}