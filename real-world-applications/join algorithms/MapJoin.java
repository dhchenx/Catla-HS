package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

//replicated join - only map side

public class MapJoin {

	static class MyMappper extends Mapper<LongWritable, Text, Text, Text> {
		private Map<String, List<String>> codeMaps = new HashMap<String, List<String>>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			BufferedReader br;
			String line = null;
			// 返回缓存文件路径

			URI[] cacheFilesPaths = context.getCacheFiles();
			for (URI path : cacheFilesPaths) {
				String pathStr = path.getPath();
				// System.out.println(pathStr);
				FileSystem fs = FileSystem.get(context.getConfiguration());

				br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathStr)), "UTF-8"));
				while (null != (line = br.readLine())) {
					// 按行读取并解析气象站数据
					String[] records = StringUtils.split(line.toString(), "\t");
					if (records != null && records.length >= 2) {
						if (!records[0].contains("_ID")) {
							System.out.println(records[1]);
							if(codeMaps.containsKey(records[1])) {
								codeMaps.get(records[1]).add(records[0]);
							}else {
								List<String> newl=new ArrayList<String>();
								newl.add(records[0]);
								codeMaps.put(records[1], newl);
							}
						 
						}
					}
				}
			}
		};

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 获取记录字符串
			String line = value.toString();
			// 抛弃空记录
			if (line == null || line.trim().isEmpty())
				return;

			String[] fs = line.split("\t");
			
			if(fs.length<2)
				return;

			if (!fs[0].contains("_ID")) {
				if (codeMaps.containsKey(fs[1])) {
					
					List<String> ids=codeMaps.get(fs[1]);
					for(int i=0;i<ids.size();i++) {
						context.write(new Text(fs[1]), new Text(ids.get(i) + "\t" + fs[0]));
					}
					
				}
			}
		}
	}

	public static String getIP(String hdfs_url) {
		String[] fs = hdfs_url.split(":");
		return fs[1].replace("/", "");
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		// test
		if (args.length == 0) {

			String jobName = "MapJoin";

			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-smalldata/codes.txt",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-data",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/output-mapjoin",
					"@traceId=" + System.currentTimeMillis(),
					"@jobName=" + jobName

			};
		}

		// obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);

		// set default if not exists @vars
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");

		if (!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, "MapJoin");

		// define the job name
	 
		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");

		// delete output folder
		FileSystem fileSystem = FileSystem.get(new URI(args[2]), conf);
		if (fileSystem.exists(new Path(args[2]))) {
			fileSystem.delete(new Path(args[2]), true);
		}

		// define job task
		Job job = Job.getInstance(conf,
				appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");
		job.setMapperClass(MyMappper.class);
		job.setJarByClass(MapJoin.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// add cache file
		job.addCacheFile(new URI(args[0]));

		// calcualte the time
		long startTime = System.currentTimeMillis(); // 获取开始时间

		job.waitForCompletion(true);

		long endTime = System.currentTimeMillis(); // 获取结束时间

		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
	}

}