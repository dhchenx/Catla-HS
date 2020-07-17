package cn.edu.bjtu.cdh.catla.stat;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.bjtu.cdh.catla.task.HadoopEnv;
import cn.edu.bjtu.cdh.catla.task.HadoopProject;
import cn.edu.bjtu.cdh.catla.task.SparkLog;
import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;

public class SparkLogStat {
	public static HadoopEnv MapToEnv(Map<String, String> map) {
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost(map.get("MasterHost"));
		he.setMasterPassword(map.get("MasterPassword"));
		he.setMasterPort(Integer.parseInt(map.get("MasterPort")));
		he.setMasterUser(map.get("MasterUser"));
		he.setHadoopBin(map.get("HadoopBin"));
		he.setAppRoot(map.get("AppRoot"));
		if(map.keySet().contains("SparkUrl"))
		{
			he.setSparkUrl(map.get("SparkUrl"));
		}
		if(map.containsKey("AppType"))
			he.setAppType(map.get("AppType"));
		else 
			he.setAppType("Hadoop");
		return he;
	}
	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length==0) {
			args=new String[] {
				"-dir","C:\\Users\\douglaschan\\Desktop\\spark\\tuning_wordcount_spark"
			};
		}
		
		Map<String, String> options = getOptionMap(args);
		
		String dirFolder=options.get("-dir");
		  HadoopProject hp = HadoopProject.createInstance(new
				  File(dirFolder).getAbsolutePath(),null);
		  
		String historyFolder=hp.getRootFolder()+"/history";
		  File file = new File(historyFolder);
	      File[] logList = file.listFiles();
	  	List<String> spark_id_list=new ArrayList<String>();
		List<String> spark_log_list=new ArrayList<String>();
	      for(int i=0;i<logList.length;i++) {
	    	  if(!logList[i].isDirectory())
	    		  continue;
	    	  if(!logList[i].getName().startsWith("log-"))
	    		  continue;
	    	  String logId=logList[i].getName().replace("log-", "").trim();
	    	  String app_id_path=logList[i].getAbsolutePath()+"/spark.app.id";
	    	  String appId=CatlaFileUtils.readFile(app_id_path).trim();
	    	  spark_id_list.add(appId);
	    	  spark_log_list.add(logId);
	    	 
	      }
	    
	      SparkLog spark_log=new SparkLog(MapToEnv(hp.getEnvMaps().get(0)));
	     	spark_log.exportSparkResult(dirFolder, "default", spark_log_list, spark_id_list);
	      
	      
	}

}
