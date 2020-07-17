package cn.edu.bjtu.cdh.catla.stat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HadoopLogStat {
	private String projectFolder;
	private Map<String, String> aliasMap;

	public HadoopLogStat(String folder) {
		this.setProjectFolder(folder);
	}

	public HadoopLogStat(String folder, Map<String, String> aliasMap) {
		this.setProjectFolder(folder);
		this.aliasMap = aliasMap;
	}

	public static String readFileAsString(String fileName) throws Exception {
		String data = "";
		data = new String(Files.readAllBytes(Paths.get(fileName)));
		return data;
	}

	private void writeFile(String path, String content) {

		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(path));
			out.write(content);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long getTimeCost(String traceId) {

		try {

			String logFolder = this.getProjectFolder() + "/history/log-" + traceId;

			if (!new File(logFolder).exists())
				return -1;

			File[] logFiles = new File(logFolder).listFiles();
			String logFile = "";
			List<String> logFileList = new ArrayList<String>();
			for (int i = 0; i < logFiles.length; i++) {
				if (logFiles[i].getName().startsWith("rumen_log_")) {
					logFile = logFiles[i].getAbsolutePath();
					logFileList.add(logFile);
				}
			}

			long time_cost = 0;

			for (int zz = 0; zz < logFileList.size(); zz++) {

				logFile = logFileList.get(zz);

				if (!new File(logFile).exists())
					break;

				String txt = readFileAsString(logFile);

				JsonObject jsonObject = new JsonParser().parse(txt).getAsJsonObject();

				String method = jsonObject.get("jobName").getAsString();

				System.out.println("Method: " + method);

				long submitTime = Long.parseLong(jsonObject.get("submitTime").getAsString());
				long launchTime = Long.parseLong(jsonObject.get("launchTime").getAsString());
				long finishTime = Long.parseLong(jsonObject.get("finishTime").getAsString());

				long sub_job_time_cost = (finishTime - submitTime); // ms
				
				System.out.println("time cost of sub job "+new File(logFile).getName()+": "+sub_job_time_cost);

				time_cost += sub_job_time_cost;

			}
			
			for(int i=0;i<logFiles.length;i++) {
				if (logFiles[i].getName().startsWith("cost_")) {
					logFile = logFiles[i].getAbsolutePath();
					new File(logFile).delete();
				}
			}
			
			
			String timeFile = this.getProjectFolder() + "/history/log-" + traceId + "/cost_" + time_cost;

			writeFile(timeFile, time_cost + "");

			return time_cost;

		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}

	}
	
	public void exportToLogFolder(String traceId) {
		String historyFolder = this.getProjectFolder() + "/history";
		System.out.println("history folder = "+historyFolder);
		File[] logFolders = new File(historyFolder).listFiles();
		List<Long> times = new ArrayList<Long>();

		times.add(Long.parseLong(traceId));

		Collections.sort(times);

		List<String> logList = new ArrayList<String>();
		List<String> argsList = new ArrayList<String>();
		
		String header = "";
		for (int i = 0; i < times.size(); i++) {
			String logFolder = historyFolder + "/log-" + times.get(i);
			File[] logFiles = new File(logFolder).listFiles();
			for (int j = 0; j < logFiles.length; j++) {
				
				if (logFiles[j].getName().startsWith("rumen_log_")) {
					logList.add(logFiles[j].getAbsolutePath());
				}

				if (logFiles[j].getName().startsWith("task_submit")) {
				
					String running_p_str = "";
					try {
						if(new File(logFolder + "/running_parameters.txt").exists()){
						running_p_str = readFileAsString(logFolder + "/running_parameters.txt");
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						continue;
					}
					String[] otherArgs = running_p_str.split("-D");
					String arg = "";
					header = "";
					for (int k = 0; k < otherArgs.length; k++) {
						if (!otherArgs[k].trim().isEmpty()) {

							System.out.println(otherArgs[k]);

							String key = otherArgs[k].substring(0, otherArgs[k].indexOf("=")).trim();
							String value = otherArgs[k].substring(otherArgs[k].indexOf("=") + 1).trim();

							if (aliasMap != null && aliasMap.containsKey(key)) {
								header += aliasMap.get(key) + "\t";
							} else {
								header += key + "\t";
							}

							if (aliasMap != null && aliasMap.containsKey(value)) {
								arg += aliasMap.get(value) + "\t";
							} else {
								arg += value + "\t";
							}

						}
					}
					arg = arg.trim();
					header = header.trim();
					argsList.add(arg);

				}

			}
		}

		// result files

		List<Long> time_cost_list = new ArrayList<Long>();
		List<Long> job_time_cost_list = new ArrayList<Long>();
		List<Long> avg_map_time_cost_list = new ArrayList<Long>();
		List<Long> avg_reduce_time_cost_list = new ArrayList<Long>();
		List<Long> avg_shuffle_time_cost_list = new ArrayList<Long>();
		List<Long> avg_sort_time_cost_list = new ArrayList<Long>();
		List<Long> setup_time_cost_list = new ArrayList<Long>();
		try {

			File fout = new File(historyFolder + "/log-"+traceId + "/stat_all.csv");
			FileOutputStream fos = new FileOutputStream(fout);

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			String jobHeader = "" + "group\t" + "method\t" + "submitTime\t" + "launchTime\t" + "finishTime\t"

					+ "task\t" + "taskNo\t" + "taskStartTime\t" + "taskFinishTime\t" + "taskInputBytes\t"
					+ "taskOutputBytes\t" + "taskInputRecords\t" + "taskOutputRecords\t"

					+ "attemptNo\t" + "aStartTime\t" + "aFinishTime\t" + "mapInputBytes\t" + "mapOutputBytes\t"
					+ "mapInputRecords\t" + "mapOutputRecords\t" + "cumulativeCpuUsage\t" + "virtualMemoryUsage\t"
					+ "physicalMemoryUsage\t" + "heapUsage\t"

					+ "combineInputRecords" + "\t" + "reduceInputGroups" + "\t" + "reduceInputRecords" + "\t"
					+ "reduceShuffleBytes" + "\t" + "reduceOutputRecords" + "\t" + "spilledRecords" + "\t"

					+ "shuffleFinished" + "\t" + "sortedFinished" + "\t" + "hostName" + "\t" + "hdfsBytesRead" + "\t"
					+ "hdfsBytesWritten" + "\t" + "fileBytesRead" + "\t" + "fileBytesWritten" + "\t"

			;

			bw.write(jobHeader.trim() + "\n");

			for (int i = 0; i < logList.size(); i++) {
				String logFile = logList.get(i);
				if (new File(logFile).isFile()) {

					String txt = readFileAsString(logList.get(i));

					JsonObject jsonObject = null;
					
					try {
						jsonObject=new JsonParser().parse(txt).getAsJsonObject();
						
					}catch(Exception ex) {
						time_cost_list.add(-1L);
						job_time_cost_list.add(-1L);
						avg_map_time_cost_list.add(-1L);
						avg_reduce_time_cost_list.add(-1L);
						avg_shuffle_time_cost_list.add(-1L);
						avg_sort_time_cost_list.add(-1L);
						setup_time_cost_list.add(-1L);
						ex.printStackTrace();
						continue;
					}
					
					if(jsonObject==null)
						continue;

					String method = jsonObject.get("jobName").getAsString();

					System.out.println("Method: " + method);

					long submitTime = Long.parseLong(jsonObject.get("submitTime").getAsString());
					long launchTime = Long.parseLong(jsonObject.get("launchTime").getAsString());
					long finishTime = Long.parseLong(jsonObject.get("finishTime").getAsString());

					long setupTime = launchTime-submitTime;
					
					setup_time_cost_list.add(setupTime);
					
					long elapsed_time_cost = (finishTime - submitTime); // ms
					long elapsed_job_time_cost=(finishTime-launchTime);

					time_cost_list.add(elapsed_time_cost);
					job_time_cost_list.add(elapsed_job_time_cost);

					long s_l = (launchTime - submitTime) / 1000;
					long f_l = (finishTime - launchTime) / 1000 + s_l;

					System.out.println(method + "\t" + "0" + "\t" + s_l + "\t" + f_l);

					JsonArray mapTasks = jsonObject.getAsJsonArray("mapTasks");
					System.out.println("Map Tasks: ");

					String jobLine = new File(projectFolder).getName() + "\t" + method + "\t" + submitTime + "\t" + launchTime + "\t" + finishTime
							+ "\t";
					List<Long> mapTimeList=new ArrayList<Long>();
					for (int j = 0; j < mapTasks.size(); j++) {

						JsonObject map = (JsonObject) mapTasks.get(j);
						System.out.println("Map " + (j + 1) + ":");
						System.out.println("startTime = " + map.get("startTime"));
						System.out.println("finishTime = " + map.get("finishTime"));
						
						long mapStartTime=Long.parseLong(map.get("startTime").getAsString());
						long mapFinishTime=Long.parseLong(map.get("finishTime").getAsString());
						
						
						
						

						System.out.println("inputBytes = " + map.get("inputBytes"));
						System.out.println("outputBytes = " + map.get("outputBytes"));

						System.out.println("inputRecords = " + map.get("inputRecords"));
						System.out.println("outputRecords = " + map.get("outputRecords"));

						JsonArray attempts = map.getAsJsonArray("attempts");

						String mapLine = jobLine + "Map" + "\t" + j + "\t" + map.get("startTime") + "\t"
								+ map.get("finishTime") + "\t" + map.get("inputBytes") + "\t" + map.get("outputBytes")
								+ "\t" + map.get("inputRecords") + "\t" + map.get("outputRecords") + "\t";

						for (int k = 0; k < attempts.size(); k++) {

							JsonObject attemp = (JsonObject) attempts.get(k);
							System.out.println("Attempt " + (k + 1) + ":");

							long map_startTime = Long.parseLong(attemp.get("startTime").getAsString());
							long map_finishTime = Long.parseLong(attemp.get("finishTime").getAsString());

							if (map_finishTime == -1)
								continue;
							
							if (attemp.get("result") == null || !attemp.get("result").getAsString().equals("SUCCESS")) {
								continue;
							}
							
							long mapAttemptTimeCost=map_finishTime-map_startTime;
							
							if(mapAttemptTimeCost>=0) {
								mapTimeList.add(mapAttemptTimeCost);
							}
								 

							String result = attemp.get("result").getAsString();

							String mapInputRecords = attemp.get("mapInputRecords").getAsString();
							String mapInputBytes = attemp.get("mapInputBytes").getAsString();
							String mapOutputBytes = attemp.get("mapOutputBytes").getAsString();
							String mapOutputRecords = attemp.get("mapOutputRecords").getAsString();

							System.out.println("mapInputRecords = " + mapInputRecords);
							System.out.println("mapInputBytes = " + mapInputBytes);
							System.out.println("mapOutputBytes = " + mapOutputBytes);
							System.out.println("mapOutputRecords = " + mapOutputRecords);

							JsonObject resMetrics = attemp.getAsJsonObject("resourceUsageMetrics");

							String cumulativeCpuUsage = resMetrics.get("cumulativeCpuUsage").getAsString();

							String virtualMemoryUsage = resMetrics.get("virtualMemoryUsage").getAsString();

							String physicalMemoryUsage = resMetrics.get("physicalMemoryUsage").getAsString();

							String heapUsage = resMetrics.get("heapUsage").getAsString();

							System.out.println("cumulativeCpuUsage = " + cumulativeCpuUsage);
							System.out.println("virtualMemoryUsage = " + virtualMemoryUsage);
							System.out.println("physicalMemoryUsage = " + physicalMemoryUsage);
							System.out.println("heapUsage = " + heapUsage);

							String attemptLine = mapLine + (k + 1) + "\t" + map_startTime + "\t" + map_finishTime + "\t"
									+ mapInputRecords + "\t" + mapInputBytes + "\t" + mapOutputBytes + "\t"
									+ mapOutputRecords + "\t" + cumulativeCpuUsage + "\t" + virtualMemoryUsage + "\t"
									+ physicalMemoryUsage + "\t" + heapUsage + "\t" + "-1" + "\t" + "-1" + "\t" + "-1"
									+ "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t"
									+ attemp.get("shuffleFinished").getAsString() + "\t"
									+ attemp.get("sortFinished").getAsString() + "\t"
									+ attemp.get("hostName").getAsString() + "\t"
									+ attemp.get("hdfsBytesRead").getAsString() + "\t"
									+ attemp.get("hdfsBytesWritten").getAsString() + "\t"
									+ attemp.get("fileBytesRead").getAsString() + "\t"
									+ attemp.get("fileBytesWritten").getAsString() + "\t";

							bw.write(attemptLine.trim() + "\n");

						}

					}
					
				

					JsonArray redcueTasks = jsonObject.getAsJsonArray("reduceTasks");
					System.out.println("\nReduce Tasks: ");
					List<Long> reduceTimeList=new ArrayList<Long>();
					List<Long> shuffleTimeList=new ArrayList<Long>();
					List<Long> sortTimeList=new ArrayList<Long>();
					for (int j = 0; j < redcueTasks.size(); j++) {

						JsonObject reduce = (JsonObject) redcueTasks.get(j);
						System.out.println("Reduce " + (j + 1) + ":");
						System.out.println("startTime = " + reduce.get("startTime"));
						System.out.println("finishTime = " + reduce.get("finishTime"));

						System.out.println("inputBytes = " + reduce.get("inputBytes"));
						System.out.println("outputBytes = " + reduce.get("outputBytes"));

						System.out.println("inputRecords = " + reduce.get("inputRecords"));
						System.out.println("outputRecords = " + reduce.get("outputRecords"));
						
						long reduce_startTime1 = Long.parseLong(reduce.get("startTime").getAsString());
						long reduce_finishTime1 = Long.parseLong(reduce.get("finishTime").getAsString());
						long reduceTimeCost=reduce_finishTime1-reduce_startTime1;
						
						  
						
						

						JsonArray attempts = reduce.getAsJsonArray("attempts");

						String reduceLine = jobLine + "Reduce" + "\t" + j + "\t" + reduce.get("startTime") + "\t"
								+ reduce.get("finishTime") + "\t" + reduce.get("inputBytes") + "\t"
								+ reduce.get("outputBytes") + "\t" + reduce.get("inputRecords") + "\t"
								+ reduce.get("outputRecords") + "\t";

						for (int k = 0; k < attempts.size(); k++) {

							JsonObject attemp = (JsonObject) attempts.get(k);
							System.out.println("Attempt " + (k + 1) + ":");

							String combineInputRecords = attemp.get("combineInputRecords").getAsString();
							String reduceInputGroups = attemp.get("reduceInputGroups").getAsString();
							String reduceInputRecords = attemp.get("reduceInputRecords").getAsString();
							String reduceShuffleBytes = attemp.get("reduceShuffleBytes").getAsString();
							String reduceOutputRecords = attemp.get("reduceOutputRecords").getAsString();
							String spilledRecords = attemp.get("spilledRecords").getAsString();

							System.out.println("combineInputRecords = " + combineInputRecords);
							System.out.println("reduceInputGroups = " + reduceInputGroups);
							System.out.println("reduceInputRecords = " + reduceInputRecords);
							System.out.println("reduceShuffleBytes = " + reduceShuffleBytes);

							System.out.println("reduceOutputRecords = " + reduceOutputRecords);
							System.out.println("spilledRecords = " + spilledRecords);

							JsonObject resMetrics = attemp.getAsJsonObject("resourceUsageMetrics");

							String cumulativeCpuUsage = resMetrics.get("cumulativeCpuUsage").getAsString();

							String virtualMemoryUsage = resMetrics.get("virtualMemoryUsage").getAsString();

							String physicalMemoryUsage = resMetrics.get("physicalMemoryUsage").getAsString();

							String heapUsage = resMetrics.get("heapUsage").getAsString();

							System.out.println("cumulativeCpuUsage = " + cumulativeCpuUsage);
							System.out.println("virtualMemoryUsage = " + virtualMemoryUsage);
							System.out.println("physicalMemoryUsage = " + physicalMemoryUsage);
							System.out.println("heapUsage = " + heapUsage);

							long reduce_startTime = Long.parseLong(attemp.get("startTime").getAsString());
							long reduce_finishTime = Long.parseLong(attemp.get("finishTime").getAsString());
							
							long reduceAttemptTimeCost=reduce_finishTime-reduce_startTime;
							
							if(reduceAttemptTimeCost>=0) {
								reduceTimeList.add(reduceAttemptTimeCost);
							}
								 

							String attemptLine = reduceLine + (k + 1) + "\t" + reduce_startTime + "\t"
									+ reduce_finishTime + "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t"
									+ cumulativeCpuUsage + "\t" + virtualMemoryUsage + "\t" + physicalMemoryUsage + "\t"
									+ heapUsage + "\t" + combineInputRecords + "\t" + reduceInputGroups + "\t"
									+ reduceInputRecords + "\t" + reduceShuffleBytes + "\t" + reduceOutputRecords + "\t"
									+ spilledRecords + "\t" + attemp.get("shuffleFinished").getAsString() + "\t"
									+ attemp.get("sortFinished").getAsString() + "\t"
									+ attemp.get("hostName").getAsString() + "\t"
									+ attemp.get("hdfsBytesRead").getAsString() + "\t"
									+ attemp.get("hdfsBytesWritten").getAsString() + "\t"
									+ attemp.get("fileBytesRead").getAsString() + "\t"
									+ attemp.get("fileBytesWritten").getAsString() + "\t";
							bw.write(attemptLine.trim() + "\n");
							
							long shuffleFinishedTime=Long.parseLong(attemp.get("shuffleFinished").getAsString());
							long shuffleTimeCost=shuffleFinishedTime-reduce_startTime;
							if(shuffleTimeCost>=0)
								shuffleTimeList.add(shuffleTimeCost);
							
							long sortedFinishedTime=Long.parseLong(attemp.get("sortFinished").getAsString());
							long sortedTimeCost=sortedFinishedTime-shuffleFinishedTime;
							if(sortedTimeCost>=0)
								sortTimeList.add(sortedTimeCost);
							

						}

					}
					
					long sum_shuffle=0;
					for(int kk=0;kk<shuffleTimeList.size();kk++)  
						sum_shuffle+=shuffleTimeList.get(kk);
					if(shuffleTimeList.size()>0)
						avg_shuffle_time_cost_list.add(sum_shuffle/shuffleTimeList.size());
					else
						avg_shuffle_time_cost_list.add(-1L);

					// System.out.println("submitTime = "+
					// jsonObject.get("submitTime").getAsString());
					
					long sum_map=0;
					for(int kk=0;kk<mapTimeList.size();kk++)  
						sum_map+=mapTimeList.get(kk);
					if(mapTimeList.size()>0)
						avg_map_time_cost_list.add(sum_map/mapTimeList.size());
					else
						avg_map_time_cost_list.add(-1L);
					
					long sum_reduce=0;
					for(int kk=0;kk<reduceTimeList.size();kk++)  
						sum_reduce+=reduceTimeList.get(kk);
					if(reduceTimeList.size()>0)
						avg_reduce_time_cost_list.add(sum_reduce/reduceTimeList.size());
					else
						avg_reduce_time_cost_list.add(-1L);
					
					long sum_sort=0;
					for(int kk=0;kk<sortTimeList.size();kk++)  
						sum_sort+=sortTimeList.get(kk);
					if(sortTimeList.size()>0)
						avg_sort_time_cost_list.add(sum_sort/sortTimeList.size());
					else
						avg_sort_time_cost_list.add(-1L);

					System.out.println();

				}

			}

			bw.close();

			// print time cost vs configuration

			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(historyFolder + "/log-"+traceId+ "/stat_timecost.csv"))));

			jobHeader = "TimeStamp\t"+ "Order" + "\t" + header + "\t" + "totalTimeCost"+"\t"+"jobTimeCost"+"\t" +"avgMapTimeCost"+"\t"+"avgReduceTimeCost\tavgShuffleTimeCost\tavgSortTimeCost\tsetupTimeCost";
			bw.write(jobHeader.trim() + "\n");

			if(times.size()>0) {
			for (int i = 0; i < times.size(); i++) {
				if (i<time_cost_list.size()&& time_cost_list.get(i) >= 0)
					bw.write(times.get(i)+"\t"+  (i + 1) + "\t" + argsList.get(i) + "\t" + time_cost_list.get(i) + "\t"+ job_time_cost_list.get(i)+ "\t"+avg_map_time_cost_list.get(i) +"\t"+avg_reduce_time_cost_list.get(i)+"\t"+avg_shuffle_time_cost_list.get(i)+ "\t"+ avg_sort_time_cost_list.get(i)+"\t"+setup_time_cost_list.get(i)+ "\n");
			}
			}

			bw.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	
	public List<Long> sumTime(List<Long> time_cost_list,List<Long> temp_time_cost_list) {
		Long sum_time_cost=0L;
		if(temp_time_cost_list.size()>0) {
			for(int kk=0;kk<temp_time_cost_list.size();kk++) {
				sum_time_cost+=temp_time_cost_list.get(kk);
			}
			Long avg_time_cost=(long)(sum_time_cost*1.0/temp_time_cost_list.size());
			time_cost_list.add(sum_time_cost);
			}else {
				time_cost_list.add(-1L);
			}
		return time_cost_list;
		
	}

	public void exportToCSV(String groupId) {
		String historyFolder = this.getProjectFolder() + "/history";
		System.out.println("history folder = "+historyFolder);
		File[] logFolders = new File(historyFolder).listFiles();
		List<Long> times = new ArrayList<Long>();
		List<List<String>> logList = new ArrayList<List<String>>();//support mutiple log aggregation
		for (int i = 0; i < logFolders.length; i++) {

			String fileName = logFolders[i].getName();
			if (!fileName.startsWith("log-"))
				continue;

			long timestamp = Long.parseLong(fileName.split("-")[1]);
			times.add(timestamp);
			logList.add(new ArrayList<String>());
		}

		Collections.sort(times);

		
		List<String> argsList = new ArrayList<String>();
		String header = "";
		for (int i = 0; i < times.size(); i++) {
			String logFolder = historyFolder + "/log-" + times.get(i);
			File[] logFiles = new File(logFolder).listFiles();
			for (int j = 0; j < logFiles.length; j++) {
				
				if (logFiles[j].getName().startsWith("rumen_log_")) {
					logList.get(i).add(logFiles[j].getAbsolutePath());
				}

				if (logFiles[j].getName().startsWith("task_submit")) {
					// taskList.add(logFiles[j].getAbsolutePath());

					// HadoopTask
					// ht=HadoopProject.MapToTask(HadoopProject.getMap(HadoopProject.readFileByLines(logFiles[j].getAbsolutePath())));

					String running_p_str = "";
					try {
						if(new File(logFolder + "/running_parameters.txt").exists()){
						running_p_str = readFileAsString(logFolder + "/running_parameters.txt");
						}
					} catch (Exception ex) {
						ex.printStackTrace();
						continue;
					}
					String[] otherArgs = running_p_str.split("-D");
					String arg = "";
					header = "";
					for (int k = 0; k < otherArgs.length; k++) {
						if (!otherArgs[k].trim().isEmpty()) {

							System.out.println(otherArgs[k]);

							String key = otherArgs[k].substring(0, otherArgs[k].indexOf("=")).trim();
							String value = otherArgs[k].substring(otherArgs[k].indexOf("=") + 1).trim();

							if (aliasMap != null && aliasMap.containsKey(key)) {
								header += aliasMap.get(key) + "\t";
							} else {
								header += key + "\t";
							}

							if (aliasMap != null && aliasMap.containsKey(value)) {
								arg += aliasMap.get(value) + "\t";
							} else {
								arg += value + "\t";
							}

						}
					}
					arg = arg.trim();
					header = header.trim();
					argsList.add(arg);

				}

			}
		}

		// result files

		List<Long> time_cost_list = new ArrayList<Long>();
		
		List<Long> job_time_cost_list = new ArrayList<Long>();
		List<Long> avg_map_time_cost_list = new ArrayList<Long>();
		List<Long> avg_reduce_time_cost_list = new ArrayList<Long>();
		List<Long> avg_shuffle_time_cost_list = new ArrayList<Long>();
		List<Long> avg_sort_time_cost_list = new ArrayList<Long>();
		List<Long> setup_time_cost_list = new ArrayList<Long>();
		
		try {

			File fout = new File(historyFolder + "/details_" + groupId + ".csv");
			FileOutputStream fos = new FileOutputStream(fout);

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

			String jobHeader = "" + "group\t" + "method\t" + "submitTime\t" + "launchTime\t" + "finishTime\t"

					+ "task\t" + "taskNo\t" + "taskStartTime\t" + "taskFinishTime\t" + "taskInputBytes\t"
					+ "taskOutputBytes\t" + "taskInputRecords\t" + "taskOutputRecords\t"

					+ "attemptNo\t" + "aStartTime\t" + "aFinishTime\t" + "mapInputBytes\t" + "mapOutputBytes\t"
					+ "mapInputRecords\t" + "mapOutputRecords\t" + "cumulativeCpuUsage\t" + "virtualMemoryUsage\t"
					+ "physicalMemoryUsage\t" + "heapUsage\t"

					+ "combineInputRecords" + "\t" + "reduceInputGroups" + "\t" + "reduceInputRecords" + "\t"
					+ "reduceShuffleBytes" + "\t" + "reduceOutputRecords" + "\t" + "spilledRecords" + "\t"

					+ "shuffleFinished" + "\t" + "sortedFinished" + "\t" + "hostName" + "\t" + "hdfsBytesRead" + "\t"
					+ "hdfsBytesWritten" + "\t" + "fileBytesRead" + "\t" + "fileBytesWritten" + "\t"

			;

			bw.write(jobHeader.trim() + "\n");

			for (int i = 0; i < times.size(); i++) {
				List<Long> temp_time_cost_list=new ArrayList<Long>();
				List<Long> temp_job_time_cost_list=new ArrayList<Long>();
				List<Long> temp_avg_map_time_cost_list=new ArrayList<Long>();
				List<Long> temp_avg_reduce_time_cost_list=new ArrayList<Long>();
				List<Long> temp_avg_shuffle_time_cost_list=new ArrayList<Long>();
				List<Long> temp_avg_sort_time_cost_list=new ArrayList<Long>();
				List<Long> temp_setup_time_cost_list=new ArrayList<Long>();
				
				
				for(int ii=0;ii<logList.get(i).size();ii++) {
				String logFile = logList.get(i).get(ii);
				
				if (new File(logFile).isFile()) {
					
					String txt = readFileAsString(logFile);

					JsonObject jsonObject = null;
					
					try {
						
						jsonObject=new JsonParser().parse(txt).getAsJsonObject();
						
					}catch(Exception ex) {
						temp_time_cost_list.add(-1L);
						temp_job_time_cost_list.add(-1L);
						temp_avg_map_time_cost_list.add(-1L);
						temp_avg_reduce_time_cost_list.add(-1L);
						temp_avg_shuffle_time_cost_list.add(-1L);
						temp_avg_sort_time_cost_list.add(-1L);
						temp_setup_time_cost_list.add(-1L);
						ex.printStackTrace();
						continue;
					}
					
					if(jsonObject==null)
						continue;

					String method = jsonObject.get("jobName").getAsString();

					System.out.println("Method: " + method);

					long submitTime = Long.parseLong(jsonObject.get("submitTime").getAsString());
					long launchTime = Long.parseLong(jsonObject.get("launchTime").getAsString());
					long finishTime = Long.parseLong(jsonObject.get("finishTime").getAsString());

					long setupTime = launchTime-submitTime;
					
					temp_setup_time_cost_list.add(setupTime);
					
					long elapsed_time_cost = (finishTime - submitTime); // ms
					long elapsed_job_time_cost=(finishTime-launchTime);

					temp_time_cost_list.add(elapsed_time_cost);
					temp_job_time_cost_list.add(elapsed_job_time_cost);

					long s_l = (launchTime - submitTime) / 1000;
					long f_l = (finishTime - launchTime) / 1000 + s_l;

					System.out.println(method + "\t" + "0" + "\t" + s_l + "\t" + f_l);

					JsonArray mapTasks = jsonObject.getAsJsonArray("mapTasks");
					System.out.println("Map Tasks: ");

					String jobLine = groupId + "\t" + method + "\t" + submitTime + "\t" + launchTime + "\t" + finishTime
							+ "\t";
					List<Long> mapTimeList=new ArrayList<Long>();
					for (int j = 0; j < mapTasks.size(); j++) {

						JsonObject map = (JsonObject) mapTasks.get(j);
						System.out.println("Map " + (j + 1) + ":");
						System.out.println("startTime = " + map.get("startTime"));
						System.out.println("finishTime = " + map.get("finishTime"));
						
						long mapStartTime=Long.parseLong(map.get("startTime").getAsString());
						long mapFinishTime=Long.parseLong(map.get("finishTime").getAsString());
						
						
						
						

						System.out.println("inputBytes = " + map.get("inputBytes"));
						System.out.println("outputBytes = " + map.get("outputBytes"));

						System.out.println("inputRecords = " + map.get("inputRecords"));
						System.out.println("outputRecords = " + map.get("outputRecords"));

						JsonArray attempts = map.getAsJsonArray("attempts");

						String mapLine = jobLine + "Map" + "\t" + j + "\t" + map.get("startTime") + "\t"
								+ map.get("finishTime") + "\t" + map.get("inputBytes") + "\t" + map.get("outputBytes")
								+ "\t" + map.get("inputRecords") + "\t" + map.get("outputRecords") + "\t";

						for (int k = 0; k < attempts.size(); k++) {

							JsonObject attemp = (JsonObject) attempts.get(k);
							System.out.println("Attempt " + (k + 1) + ":");

							long map_startTime = Long.parseLong(attemp.get("startTime").getAsString());
							long map_finishTime = Long.parseLong(attemp.get("finishTime").getAsString());

							if (map_finishTime == -1)
								continue;
							
							if (attemp.get("result") == null || !attemp.get("result").getAsString().equals("SUCCESS")) {
								continue;
							}
							
							long mapAttemptTimeCost=map_finishTime-map_startTime;
							
							if(mapAttemptTimeCost>=0) {
								mapTimeList.add(mapAttemptTimeCost);
							}
								 

							String result = attemp.get("result").getAsString();

							String mapInputRecords = attemp.get("mapInputRecords").getAsString();
							String mapInputBytes = attemp.get("mapInputBytes").getAsString();
							String mapOutputBytes = attemp.get("mapOutputBytes").getAsString();
							String mapOutputRecords = attemp.get("mapOutputRecords").getAsString();

							System.out.println("mapInputRecords = " + mapInputRecords);
							System.out.println("mapInputBytes = " + mapInputBytes);
							System.out.println("mapOutputBytes = " + mapOutputBytes);
							System.out.println("mapOutputRecords = " + mapOutputRecords);

							JsonObject resMetrics = attemp.getAsJsonObject("resourceUsageMetrics");

							String cumulativeCpuUsage = resMetrics.get("cumulativeCpuUsage").getAsString();

							String virtualMemoryUsage = resMetrics.get("virtualMemoryUsage").getAsString();

							String physicalMemoryUsage = resMetrics.get("physicalMemoryUsage").getAsString();

							String heapUsage = resMetrics.get("heapUsage").getAsString();

							System.out.println("cumulativeCpuUsage = " + cumulativeCpuUsage);
							System.out.println("virtualMemoryUsage = " + virtualMemoryUsage);
							System.out.println("physicalMemoryUsage = " + physicalMemoryUsage);
							System.out.println("heapUsage = " + heapUsage);

							String attemptLine = mapLine + (k + 1) + "\t" + map_startTime + "\t" + map_finishTime + "\t"
									+ mapInputRecords + "\t" + mapInputBytes + "\t" + mapOutputBytes + "\t"
									+ mapOutputRecords + "\t" + cumulativeCpuUsage + "\t" + virtualMemoryUsage + "\t"
									+ physicalMemoryUsage + "\t" + heapUsage + "\t" + "-1" + "\t" + "-1" + "\t" + "-1"
									+ "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t"
									+ attemp.get("shuffleFinished").getAsString() + "\t"
									+ attemp.get("sortFinished").getAsString() + "\t"
									+ attemp.get("hostName").getAsString() + "\t"
									+ attemp.get("hdfsBytesRead").getAsString() + "\t"
									+ attemp.get("hdfsBytesWritten").getAsString() + "\t"
									+ attemp.get("fileBytesRead").getAsString() + "\t"
									+ attemp.get("fileBytesWritten").getAsString() + "\t";

							bw.write(attemptLine.trim() + "\n");

						}
					
					}
					
				

					JsonArray redcueTasks = jsonObject.getAsJsonArray("reduceTasks");
					System.out.println("\nReduce Tasks: ");
					List<Long> reduceTimeList=new ArrayList<Long>();
					List<Long> shuffleTimeList=new ArrayList<Long>();
					List<Long> sortTimeList=new ArrayList<Long>();
					for (int j = 0; j < redcueTasks.size(); j++) {

						JsonObject reduce = (JsonObject) redcueTasks.get(j);
						System.out.println("Reduce " + (j + 1) + ":");
						System.out.println("startTime = " + reduce.get("startTime"));
						System.out.println("finishTime = " + reduce.get("finishTime"));

						System.out.println("inputBytes = " + reduce.get("inputBytes"));
						System.out.println("outputBytes = " + reduce.get("outputBytes"));

						System.out.println("inputRecords = " + reduce.get("inputRecords"));
						System.out.println("outputRecords = " + reduce.get("outputRecords"));
						
						long reduce_startTime1 = Long.parseLong(reduce.get("startTime").getAsString());
						long reduce_finishTime1 = Long.parseLong(reduce.get("finishTime").getAsString());
						long reduceTimeCost=reduce_finishTime1-reduce_startTime1;
						
						  
						
						

						JsonArray attempts = reduce.getAsJsonArray("attempts");

						String reduceLine = jobLine + "Reduce" + "\t" + j + "\t" + reduce.get("startTime") + "\t"
								+ reduce.get("finishTime") + "\t" + reduce.get("inputBytes") + "\t"
								+ reduce.get("outputBytes") + "\t" + reduce.get("inputRecords") + "\t"
								+ reduce.get("outputRecords") + "\t";

						for (int k = 0; k < attempts.size(); k++) {

							JsonObject attemp = (JsonObject) attempts.get(k);
							System.out.println("Attempt " + (k + 1) + ":");

							String combineInputRecords = attemp.get("combineInputRecords").getAsString();
							String reduceInputGroups = attemp.get("reduceInputGroups").getAsString();
							String reduceInputRecords = attemp.get("reduceInputRecords").getAsString();
							String reduceShuffleBytes = attemp.get("reduceShuffleBytes").getAsString();
							String reduceOutputRecords = attemp.get("reduceOutputRecords").getAsString();
							String spilledRecords = attemp.get("spilledRecords").getAsString();

							System.out.println("combineInputRecords = " + combineInputRecords);
							System.out.println("reduceInputGroups = " + reduceInputGroups);
							System.out.println("reduceInputRecords = " + reduceInputRecords);
							System.out.println("reduceShuffleBytes = " + reduceShuffleBytes);

							System.out.println("reduceOutputRecords = " + reduceOutputRecords);
							System.out.println("spilledRecords = " + spilledRecords);

							JsonObject resMetrics = attemp.getAsJsonObject("resourceUsageMetrics");

							String cumulativeCpuUsage = resMetrics.get("cumulativeCpuUsage").getAsString();

							String virtualMemoryUsage = resMetrics.get("virtualMemoryUsage").getAsString();

							String physicalMemoryUsage = resMetrics.get("physicalMemoryUsage").getAsString();

							String heapUsage = resMetrics.get("heapUsage").getAsString();

							System.out.println("cumulativeCpuUsage = " + cumulativeCpuUsage);
							System.out.println("virtualMemoryUsage = " + virtualMemoryUsage);
							System.out.println("physicalMemoryUsage = " + physicalMemoryUsage);
							System.out.println("heapUsage = " + heapUsage);

							long reduce_startTime = Long.parseLong(attemp.get("startTime").getAsString());
							long reduce_finishTime = Long.parseLong(attemp.get("finishTime").getAsString());
							
							long reduceAttemptTimeCost=reduce_finishTime-reduce_startTime;
							
							if(reduceAttemptTimeCost>=0) {
								reduceTimeList.add(reduceAttemptTimeCost);
							}
								 

							String attemptLine = reduceLine + (k + 1) + "\t" + reduce_startTime + "\t"
									+ reduce_finishTime + "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t" + "-1" + "\t"
									+ cumulativeCpuUsage + "\t" + virtualMemoryUsage + "\t" + physicalMemoryUsage + "\t"
									+ heapUsage + "\t" + combineInputRecords + "\t" + reduceInputGroups + "\t"
									+ reduceInputRecords + "\t" + reduceShuffleBytes + "\t" + reduceOutputRecords + "\t"
									+ spilledRecords + "\t" + attemp.get("shuffleFinished").getAsString() + "\t"
									+ attemp.get("sortFinished").getAsString() + "\t"
									+ attemp.get("hostName").getAsString() + "\t"
									+ attemp.get("hdfsBytesRead").getAsString() + "\t"
									+ attemp.get("hdfsBytesWritten").getAsString() + "\t"
									+ attemp.get("fileBytesRead").getAsString() + "\t"
									+ attemp.get("fileBytesWritten").getAsString() + "\t";
							bw.write(attemptLine.trim() + "\n");
							
							long shuffleFinishedTime=Long.parseLong(attemp.get("shuffleFinished").getAsString());
							long shuffleTimeCost=shuffleFinishedTime-reduce_startTime;
							if(shuffleTimeCost>=0)
								shuffleTimeList.add(shuffleTimeCost);
							
							long sortedFinishedTime=Long.parseLong(attemp.get("sortFinished").getAsString());
							long sortedTimeCost=sortedFinishedTime-shuffleFinishedTime;
							if(sortedTimeCost>=0)
								sortTimeList.add(sortedTimeCost);
							

						}

					}
					
					long sum_shuffle=0;
					for(int kk=0;kk<shuffleTimeList.size();kk++)  
						sum_shuffle+=shuffleTimeList.get(kk);
					if(shuffleTimeList.size()>0)
						temp_avg_shuffle_time_cost_list.add(sum_shuffle/shuffleTimeList.size());
					else
						temp_avg_shuffle_time_cost_list.add(-1L);

					// System.out.println("submitTime = "+
					// jsonObject.get("submitTime").getAsString());
					
					long sum_map=0;
					for(int kk=0;kk<mapTimeList.size();kk++)  
						sum_map+=mapTimeList.get(kk);
					if(mapTimeList.size()>0)
						temp_avg_map_time_cost_list.add(sum_map/mapTimeList.size());
					else
						temp_avg_map_time_cost_list.add(-1L);
					
					long sum_reduce=0;
					for(int kk=0;kk<reduceTimeList.size();kk++)  
						sum_reduce+=reduceTimeList.get(kk);
					if(reduceTimeList.size()>0)
						temp_avg_reduce_time_cost_list.add(sum_reduce/reduceTimeList.size());
					else
						temp_avg_reduce_time_cost_list.add(-1L);
					
					long sum_sort=0;
					for(int kk=0;kk<sortTimeList.size();kk++)  
						sum_sort+=sortTimeList.get(kk);
					if(sortTimeList.size()>0)
						temp_avg_sort_time_cost_list.add(sum_sort/sortTimeList.size());
					else
						temp_avg_sort_time_cost_list.add(-1L);

					System.out.println();

				}
				
				}
				
				//average time cost
				Long avg_time_cost=0L;
				Long sum_time_cost=0L;
				if(temp_time_cost_list.size()>0) {
				for(int kk=0;kk<temp_time_cost_list.size();kk++) {
					sum_time_cost+=temp_time_cost_list.get(kk);
				}
				avg_time_cost=(long)(sum_time_cost*1.0/temp_time_cost_list.size());
				time_cost_list.add(sum_time_cost);
				}else {
					time_cost_list.add(-1L);
				}
				
			
				
				job_time_cost_list=sumTime(job_time_cost_list,temp_job_time_cost_list);
				avg_map_time_cost_list=sumTime(avg_map_time_cost_list,temp_avg_map_time_cost_list);
				avg_reduce_time_cost_list=sumTime(avg_reduce_time_cost_list,temp_avg_reduce_time_cost_list);
				avg_shuffle_time_cost_list=sumTime(avg_shuffle_time_cost_list,temp_avg_shuffle_time_cost_list);
				avg_sort_time_cost_list=sumTime(avg_sort_time_cost_list,temp_avg_sort_time_cost_list);
				
				setup_time_cost_list=sumTime(setup_time_cost_list,temp_setup_time_cost_list);
				
			}

			bw.close();

			// print time cost vs configuration

			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(historyFolder + "/timecost_" + groupId + ".csv"))));

			jobHeader = "TimeStamp\t"+ "Order" + "\t" + header + "\t" + "totalTimeCost"+"\t"+"jobTimeCost"+"\t" +"avgMapTimeCost"+"\t"+"avgReduceTimeCost\tavgShuffleTimeCost\tavgSortTimeCost\tsetupTimeCost";
			bw.write(jobHeader.trim() + "\n");
			System.out.println("Timestamp list");
			for(int i=0;i<times.size();i++) {
				System.out.print(times.get(i)+"\t");
			}
			System.out.println();
			System.out.println("Timecost list");
			for(int i=0;i<time_cost_list.size();i++) {
				System.out.print(time_cost_list.get(i)+"\t");
			}
			
			
			System.out.println();
			
			
			if(times.size()>0) {
			for (int i = 0; i < times.size(); i++) {
				
				if (i<time_cost_list.size()&& time_cost_list.get(i) >= 0)
					bw.write(times.get(i)+"\t"+  (i + 1) + "\t" + argsList.get(i) + "\t" + time_cost_list.get(i) + "\t"+ job_time_cost_list.get(i)+ "\t"+avg_map_time_cost_list.get(i) +"\t"+avg_reduce_time_cost_list.get(i)+"\t"+avg_shuffle_time_cost_list.get(i)+ "\t"+ avg_sort_time_cost_list.get(i)+"\t"+setup_time_cost_list.get(i)+ "\n");
			}
			}

			bw.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	
	public String getJobHeaders() {
		return "TimeStamp\t"+ "Order" + "\t" + "totalTimeCost"+"\t"+"jobTimeCost"+"\t" +"avgMapTimeCost"+"\t"+"avgReduceTimeCost\tavgShuffleTimeCost\tavgSortTimeCost\tsetupTimeCost";
		
	}

	public String getProjectFolder() {
		return projectFolder;
	}

	public void setProjectFolder(String projectFolder) {
		this.projectFolder = projectFolder;
	}
	
	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}

	public static void main(String[] args) throws Exception {
		/*
		Map<String, String> map = new HashMap<String, String>();
		map.put("/data/cdh/research/join-test/input-smalldata /data/cdh/research/join-test/input-data /data/cdh/research/join-test/output-reducejoin @jointype=inner",
				"normal");
				*/
		if(args.length==0) {
			args=new String[] {
				"-dir",	"C:\\Resources\\我的项目\\PhD\\博士论文\\大数据算法测试\\join\\large-small\\cdh_mr_mapjoin"
			};
		}
		
		Map<String, String> options = getOptionMap(args);
		HadoopLogStat tl = new HadoopLogStat(options.get("-dir"), null);
		tl.exportToCSV(new File(options.get("-dir")).getName());
		
	}

}
