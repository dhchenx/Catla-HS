package cn.edu.bjtu.cdh.bigdata.research.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import cn.edu.bjtu.cdh.bigdata.research.utils.InjectVars;

public class ReplicatedJoin {

	/*
	 * A replicated join is a special type of join operation between one large and
	 * many small data sets that can be performed on the map-side. This pattern
	 * completely eliminates the need to shuffle any data to the reduce phase.
	 */

	/*
	 * A replicated join is an extremely useful, but has a strict size limit on all
	 * but one of the data sets to be joined. All the data sets except the very
	 * large one are essentially read into memory during the setup phase of each map
	 * task, which is limited by the JVM heap. If you can live within this
	 * limitation, you get a drastic benefit because there is no reduce phase at
	 * all, and therefore no shuffling or sorting. The join is done entirely in the
	 * map phase, with the very large data set being the input for the MapReduce
	 * job.
	 */

	/*
	 * There is an additional restriction that a replicated join is really useful
	 * only for an inner or a left outer join where the large data set is the “left”
	 * data set. The other join types require a reduce phase to group the “right”
	 * data set with the entirety of the left data set. Although there may not be a
	 * match for the data stored in memory for a given map task, there could be
	 * match in another input split. Because of this, we will restrict this pattern
	 * to inner and left outer joins.
	 */

	/*
	 * The type of join to execute is an inner join or a left outer join, with the
	 * large input data set being the “left” part of the operation. All of the data
	 * sets, except for the large one, can be fit into main memory of each map task.
	 */

	public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, NullWritable> {

		HashMap<String, String> inMemoryUsers;
		String joinType;

		public void setup(Context context) throws IOException {

			inMemoryUsers = new HashMap<String, String>();
			
			URI[] cacheFilesPaths = context.getCacheFiles();

			joinType = context.getConfiguration().get("join.type");
		
			if (cacheFilesPaths == null) {
				String msg = "Could not find files in distributed cache";

				throw new IOException(msg);
			}

			for (URI path : cacheFilesPaths) {
				String pathStr = path.getPath();
				// System.out.println(pathStr);
				FileSystem fs = FileSystem.get(context.getConfiguration());
				String line=null;
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathStr)), "UTF-8"));
				while (null != (line = br.readLine())) {
					// 按行读取并解析气象站数据
					String[] records = StringUtils.split(line.toString(), "\t");
					if (records != null && records.length >= 2) {
						if (!records[0].contains("_ID")) {
							System.out.println(records[1]);
							inMemoryUsers.put(records[1], records[0]);
						}
					}
				}
			}

		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] values = value.toString().split("\t");
			 
		
			
			if(value.toString().isEmpty()||value.toString().contains("_ID"))
					return;
			
			if(values.length<2)
				return;
			
			String joinValue = inMemoryUsers.get(values[1]);
			Text output = new Text();

			if (joinType.equalsIgnoreCase("inner")) {
				// Only if user is known
				if (joinValue != null && !joinValue.isEmpty()) {
					output.set(value.toString() + "\t" + joinValue);
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// Even if user is not known
				if (joinValue == null || joinValue.isEmpty()) {
					output.set(value.toString());
				} else {
					output.set(value.toString() + "\t" + joinValue);
				}
			}
			context.write(output, NullWritable.get());
		}

	}

	public static class ReplicatedJoinReducer extends Reducer<Text, Iterable<NullWritable>, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static String getIP(String hdfs_url) {
		String[] fs = hdfs_url.split(":");
		return fs[1].replace("/", "");
	}

	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		String jobName = "ReplicatedJoin";
		Configuration conf = new Configuration();
		
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length == 0) {

		
			args = new String[] { "hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-smalldata", // small
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/input-data",
					"hdfs://192.168.159.132:9000/data/cdh/research/join-realdata/output-replicatedjoin", "@jointype=inner",
					"@traceId=" + System.currentTimeMillis(), "@jobName=" + jobName };
		}

		// obtain application args
		Map<String, String> appArgs = InjectVars.getVars(args);
		args = InjectVars.getArgs(args);
		
		// set default if not exists @vars
		if (!appArgs.containsKey(InjectVars.traceId))
			appArgs.put(InjectVars.traceId, "0");

		if (!appArgs.containsKey(InjectVars.jobName))
			appArgs.put(InjectVars.jobName, jobName);

	 
		
		conf.set("mapred.jop.tracker", "hdfs://" + getIP(args[0]) + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + getIP(args[0]) + ":9000");

// Small dataset to Join
		FileSystem hdfs = FileSystem.get(conf);
 
		if (hdfs.exists(new Path(args[2]))) {
			hdfs.delete(new Path(args[2]), true);
		}
		
// Job must be created AFTER you add files to cache
		Job job = Job.getInstance(conf,
				appArgs.get(InjectVars.jobName) + "-" + "[" + appArgs.get(InjectVars.traceId) + "]");

// Full dataset to browse
		TextInputFormat.addInputPath(job, new Path(args[1]));

		job.setJarByClass(ReplicatedJoin.class);
		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setReducerClass(ReplicatedJoinReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.getConfiguration().set("join.type", appArgs.get("jointype"));

		RemoteIterator<LocatedFileStatus> ri = hdfs.listFiles(new Path(args[0]), true);
		boolean atLeastOne = false;
		while (ri.hasNext()) {
			LocatedFileStatus lfs = ri.next();
			Path file = lfs.getPath();
			System.out.println("Adding file " + file.toString() + " to distributed cache");

			job.addCacheFile(new URI("" + file));

			atLeastOne = true;
		}

		if (!atLeastOne) {
			String msg = "Was not able to add any file to distributed cache";

			throw new IOException(msg);
		}
		long startTime = System.currentTimeMillis(); // 获取开始时间

		int code = job.waitForCompletion(true) ? 0 : 1;
	
		
		long endTime = System.currentTimeMillis(); // 获取结束时间
		System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
	}

}