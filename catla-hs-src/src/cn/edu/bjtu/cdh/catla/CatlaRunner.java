package cn.edu.bjtu.cdh.catla;

import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cn.edu.bjtu.cdh.catla.optimization.HadoopOptimizer;
import cn.edu.bjtu.cdh.catla.stat.HadoopLogStat;
import cn.edu.bjtu.cdh.catla.stat.SparkLogStat;
import cn.edu.bjtu.cdh.catla.task.HadoopLog;
import cn.edu.bjtu.cdh.catla.task.HadoopProject;
import cn.edu.bjtu.cdh.catla.task.HadoopTask;
import cn.edu.bjtu.cdh.catla.tuning.HadoopTuning;

public class CatlaRunner {
	
	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}

	public static void main(String[] args){

		try {
		
		Logger.getRootLogger().setLevel(Level.OFF);
		 
		System.out.println("Catla: A self-tuning system for Hadoop parameters to improve MapReduce job performance");
		System.out.println("Developed By Donghua Chen");
		System.out.println("");

		URL rootUrl = CatlaRunner.class.getProtectionDomain().getCodeSource().getLocation();
		String jarFolder = URLDecoder.decode(rootUrl.getPath(), "utf-8");

		if (jarFolder.endsWith(".jar")) {
			jarFolder = jarFolder.substring(0, jarFolder.lastIndexOf("/"));
			if (jarFolder.contains(":"))
				jarFolder = jarFolder.substring(1);
				
		} else {
			jarFolder = "";
		}

		System.out.println("Runnable Jar Folder = " + jarFolder);

		

		Map<String, String> options = getOptionMap(args);

		if (options.containsKey("-tool")) {
			
			if (options.get("-tool").equals("task")) {
				HadoopTask.main(args);
			}else
			if (options.get("-tool").equals("project")) {
				HadoopProject.main(args);
			}else
			if (options.get("-tool").equals("tuning")) {
				HadoopTuning.main(args);
			}else
			if (options.get("-tool").equals("optimizer")) {
				HadoopOptimizer.main(args);
			}else if(options.get("-tool").equals("log")) {
				HadoopLogStat.main(args);
			}else if(options.get("-tool").equals("refreshlog")) {
				HadoopLog.main(args);
			}else if(options.get("-tool").equals("spark_log_stat")) {
				SparkLogStat.main(args);
			}else if(options.get("-tool").equals("hadoop_log_stat")) {
				HadoopLogStat.main(args);
			}else {
				System.out.println("Error: no valid -tool value is pointed!");
			}

		} else {
			System.out.println("Error: please specify -tool paramters!");
		}
		}catch(Exception ex) {
			ex.printStackTrace();
		}

	}
}
