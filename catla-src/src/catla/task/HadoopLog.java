package cn.edu.bjtu.cdh.catla.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import cn.edu.bjtu.cdh.catla.tuning.TuningLog;
import cn.edu.bjtu.cdh.catla.utils.UnicodeReader;

public class HadoopLog {
	private HadoopEnv env;
	public HadoopLog(HadoopEnv he) {
		this.env=he;
		
	}
	
	public HadoopLog(String projectFolder) {
		HadoopProject hp = HadoopProject.createInstance(projectFolder);
		this.env=MapToEnv(hp.getEnvMaps().get(0));
	}
	
	public static List<String> readFileByLines(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		List<String> lines = new ArrayList<String>();
		try {

			UnicodeReader read = new UnicodeReader(new FileInputStream(file), "UTF-8");
			reader = new BufferedReader(read);

			String tempString = null;
			int line = 1;
		
			while ((tempString = reader.readLine()) != null) {
			
				System.out.println(tempString);
				if (!tempString.trim().isEmpty() && !tempString.startsWith("#"))
					lines.add(tempString);
				line++;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return lines;
	}
	
	public static Map<String, String> getMap(List<String> lines) {
		Map<String, String> map = new HashMap<String, String>();

		for (String s : lines) {
			String[] ls = s.split(" ");
			String key = ls[0];
			String rest = "";
			for (int i = 1; i < ls.length; i++) {
				if (i != ls.length - 1) {
					rest += ls[i] + " ";
				} else
					rest += ls[i];
			}

			if (!key.isEmpty())
				map.put(key, rest);
		}
		return map;
	}

	public static HadoopEnv MapToEnv(Map<String, String> map) {
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost(map.get("MasterHost"));
		he.setMasterPassword(map.get("MasterPassword"));
		he.setMasterPort(Integer.parseInt(map.get("MasterPort")));
		he.setMasterUser(map.get("MasterUser"));
		he.setHadoopBin(map.get("HadoopBin"));
		he.setAppRoot(map.get("AppRoot"));
		return he;
	}
	
	private String stagingPath="/tmp/hadoop-yarn/staging/hadoop/.staging";
	private String doneIntermediatePath="/tmp/hadoop-yarn/staging/history/done_intermediate/hadoop";
	private String donePath="/tmp/hadoop-yarn/staging/history/done";
	
	public HadoopEnv getEnv() {
		return env;
	}
	public void setEnv(HadoopEnv env) {
		this.env = env;
	}
	public String getStagingPath() {
		return stagingPath;
	}
	public void setStagingPath(String stagingPath) {
		this.stagingPath = stagingPath;
	}
	public String getDoneIntermediatePath() {
		return doneIntermediatePath;
	}
	public void setDoneIntermediatePath(String doneIntermediatePath) {
		this.doneIntermediatePath = doneIntermediatePath;
	}
	public String getDonePath() {
		return donePath;
	}
	public void setDonePath(String donePath) {
		this.donePath = donePath;
	}
	
	public static class MyAllPathFilter implements PathFilter{
		private String traceId;
		public MyAllPathFilter(String traceId) {
			this.traceId=traceId;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			String pathStr=path.toString();
        	
        	try {
        		pathStr=java.net.URLDecoder.decode(pathStr, "utf-8");
        	}catch(Exception ex) {
        		ex.printStackTrace();
        	}
        			//System.out.println(pathStr);
        	if(pathStr.contains("[")&&pathStr.contains("]")) {
        		int b_i=pathStr.indexOf("[");
        		int e_i=pathStr.indexOf("]");
        		
        		String traceFullName=pathStr.substring(b_i+1,e_i);
        		String trace_id="";
        		if(traceFullName.contains("-")) {
        			trace_id=traceFullName.split("-")[0];
        		}else {
        			trace_id=traceFullName;
        		}
        		
        		if(trace_id.equals(this.traceId)) {
        	 
        		   return true;
        		     
        		}
        	 
        	}
        	return false;
			 
		}
		
	}
	
	public static class MyDonePathFilter implements PathFilter{
		private String traceId;
		public MyDonePathFilter(String traceId) {
			this.traceId=traceId;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			String pathStr=path.toString();
        	
        	try {
        		pathStr=java.net.URLDecoder.decode(pathStr, "utf-8");
        	}catch(Exception ex) {
        		ex.printStackTrace();
        	}
        			//System.out.println(pathStr);
        	if(pathStr.contains("[")&&pathStr.contains("]")) {
        		int b_i=pathStr.indexOf("[");
        		int e_i=pathStr.indexOf("]");
        		
        		String traceFullName=pathStr.substring(b_i+1,e_i);
        		String trace_id="";
        		
        		if(traceFullName.contains("-")) {
        			trace_id=traceFullName.split("-")[0];
        		}else {
        			trace_id=traceFullName;
        		}
        		
        		if(trace_id.equals(this.traceId)) {
        		    if(traceFullName.contains("-")&&traceFullName.endsWith("END"))
        		    	return true;
        		    else if(!traceFullName.contains("-")){
        		    	return true;
        		    }
        		}
        	 
        	}
        	return false;
			 
		}
		
	}
	
	public static String getTraceId(String path) {
		String pathStr=path.toString();
    	
    	try {
     pathStr=java.net.URLDecoder.decode(pathStr, "utf-8");
    	}catch(Exception ex) {
    		ex.printStackTrace();
    	}
    			//System.out.println(pathStr);
    	if(pathStr.contains("[")&&pathStr.contains("]")) {
    		int b_i=pathStr.indexOf("[");
    		int e_i=pathStr.indexOf("]");
    		
    		String trace_id=pathStr.substring(b_i+1,e_i);
    	 
    		return trace_id;
    	 
    	}
    	return "";
	}
	
	private void writeFromHDFS2Local(FileSystem fs,String CLOUD_DESC,String LOCAL_SRC) {
		try {
		// 读出流
		FSDataInputStream HDFS_IN = fs.open(new org.apache.hadoop.fs.Path(CLOUD_DESC));
		// 写入流
		OutputStream OutToLOCAL = new FileOutputStream(LOCAL_SRC);
		// 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
		IOUtils.copyBytes(HDFS_IN, OutToLOCAL, 1024, true);
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public boolean downloadDoneIntermediateJob(String traceOutputFolder,String traceId) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
			  FileSystem hdfs = FileSystem.get(conf);
			  
			  org.apache.hadoop.fs.Path donePath = new org.apache.hadoop.fs.Path(this.getDoneIntermediatePath()+"/*");

	         FileStatus[] doneFiles = hdfs.globStatus(donePath,new MyAllPathFilter(traceId));

	         if(doneFiles!=null&&doneFiles.length>0)
	         {
	        	// String traceFolder="C:/Resources/我的项目/PhD/博士论文/大数据Tuning/trace";
	        	 String traceFolder=traceOutputFolder;
	        	 
	        	 org.apache.hadoop.fs.Path[] logPaths = FileUtil.stat2Paths(doneFiles);
	 	          
	        	 
	        	 
	        	 List<String> logList=new ArrayList<String>();
	        	 List<String> confList=new ArrayList<String>();
	        	 
	        	 List<String> traceList=new ArrayList<String>();
	        	 List<String> toList=new ArrayList<String>();
	        	 
	        	 
	        	  for (int i=0;i<logPaths.length;i++){
		            	org.apache.hadoop.fs.Path p =logPaths[i];
		            	String log_name=""+p;
		            	logList.add(log_name);
		            	
		            
		            	String[] ls=p.getName().split("-");
		            	String conf_name=ls[0]+"_conf.xml";
		            	
		            	String conf_path=p.getParent()+"/"+conf_name;
		            	confList.add(conf_path);
		           
		            	
		            	org.apache.hadoop.tools.rumen.TraceBuilder tb=new org.apache.hadoop.tools.rumen.TraceBuilder();
		            	
		            	String myId=getTraceId(log_name);
		            	String sub_id=i+"";;
		            	if(myId.contains("-")) {
		            		sub_id=myId.split("-")[1];
		            		myId=myId.substring(0, myId.indexOf("-"));
		            	}
		            	
		            	
		            	String traceFile=p.getParent()+"/rumen_log_"+myId+"_"+sub_id+".json";
		            	String toFile=p.getParent()+"/rumen_topology_"+myId+"_"+sub_id+".json";
		            	
		            	traceList.add(traceFile);
		            	toList.add(toFile);
		            	
		            	System.out.println("traceFile="+traceFile);
		            	System.out.println("toFile="+toFile);
		            	
		            	System.out.println("log name="+log_name);
		            	System.out.println("conf path="+conf_path);
		            
		        		int r=ToolRunner.run(tb, new String[] {traceFile,toFile, log_name});
		
		        		this.writeFromHDFS2Local(hdfs, traceFile, traceFolder+"/"+new File(traceFile).getName());
		        		this.writeFromHDFS2Local(hdfs, toFile, traceFolder+"/"+new File(toFile).getName());
		        		//print law
		        		this.writeFromHDFS2Local(hdfs, conf_path, traceFolder+"/"+new File(conf_path).getName());
		        		this.writeFromHDFS2Local(hdfs, log_name, traceFolder+"/"+new File(log_name).getName());
		            	
		        		hdfs.delete(new Path(traceFile),true);
		        		hdfs.delete(new Path(toFile),true);
		        		
		            	System.out.println("RUMEN RESTULT="+r);
		            	
		            	
		            }
	        	 
	        	 
	         }
 
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		
			return false;
		
	}
	
	public boolean downloadDoneJob(String traceOutputFolder,String traceId) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
			  FileSystem hdfs = FileSystem.get(conf);
			  
			  org.apache.hadoop.fs.Path donePath = new org.apache.hadoop.fs.Path(this.getDonePath()+ "/*/*/*/*/*");

	         FileStatus[] doneFiles = hdfs.globStatus(donePath,new MyAllPathFilter(traceId));

	         if(doneFiles!=null&&doneFiles.length>0)
	         {
	        	// String traceFolder="C:/Resources/我的项目/PhD/博士论文/大数据Tuning/trace";
	        	 String traceFolder=traceOutputFolder;
	        	 
	        	 org.apache.hadoop.fs.Path[] logPaths = FileUtil.stat2Paths(doneFiles);
	 	          
	        	 
	        	 
	        	 List<String> logList=new ArrayList<String>();
	        	 List<String> confList=new ArrayList<String>();
	        	 
	        	 List<String> traceList=new ArrayList<String>();
	        	 List<String> toList=new ArrayList<String>();
	        	 
	        	 
	        	  for (int i=0;i<logPaths.length;i++){
		            	org.apache.hadoop.fs.Path p =logPaths[i];
		            	String log_name=""+p;
		            	logList.add(log_name);
		            	
		            
		            	String[] ls=p.getName().split("-");
		            	String conf_name=ls[0]+"_conf.xml";
		            	
		            	String conf_path=p.getParent()+"/"+conf_name;
		            	confList.add(conf_path);
		           
		            	
		            	org.apache.hadoop.tools.rumen.TraceBuilder tb=new org.apache.hadoop.tools.rumen.TraceBuilder();
		            	
		            	String myId=getTraceId(log_name);
		            	String sub_id=i+"";;
		            	if(myId.contains("-")) {
		            		sub_id=myId.split("-")[1];
		            		myId=myId.substring(0, myId.indexOf("-"));
		            	}
		            	
		            	
		            	String traceFile=p.getParent()+"/rumen_log_"+myId+"_"+sub_id+".json";
		            	String toFile=p.getParent()+"/rumen_topology_"+myId+"_"+sub_id+".json";
		            	
		            	traceList.add(traceFile);
		            	toList.add(toFile);
		            	
		            	System.out.println("traceFile="+traceFile);
		            	System.out.println("toFile="+toFile);
		            	
		            	System.out.println("log name="+log_name);
		            	System.out.println("conf path="+conf_path);
		            
		        		int r=ToolRunner.run(tb, new String[] {traceFile,toFile, log_name});
		
		        		this.writeFromHDFS2Local(hdfs, traceFile, traceFolder+"/"+new File(traceFile).getName());
		        		this.writeFromHDFS2Local(hdfs, toFile, traceFolder+"/"+new File(toFile).getName());
		        		//print law
		        		this.writeFromHDFS2Local(hdfs, conf_path, traceFolder+"/"+new File(conf_path).getName());
		        		this.writeFromHDFS2Local(hdfs, log_name, traceFolder+"/"+new File(log_name).getName());
		            	
		            	System.out.println("RUMEN RESTULT="+r);
		            	
		            	
		            }
	        	 
	        	 
	         }
 
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		
			return false;
		
	}
	
	public boolean checkExistsDoneLog(String traceId) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
			  FileSystem hdfs = FileSystem.get(conf);
			  
			  org.apache.hadoop.fs.Path donePath = new org.apache.hadoop.fs.Path(this.getDonePath()+ "/*/*/*/*/*");

	         FileStatus[] doneFiles = hdfs.globStatus(donePath,new MyDonePathFilter(traceId));

	         if(doneFiles!=null&&doneFiles.length>0)
	        	 return true;
 
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		
			return false;
	}
	
	public boolean checkExistsDoneIntermediateLog(String traceId) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
			  FileSystem hdfs = FileSystem.get(conf);
			  
			  org.apache.hadoop.fs.Path donePath = new org.apache.hadoop.fs.Path(this.getDoneIntermediatePath()+ "/*");

	         FileStatus[] doneFiles = hdfs.globStatus(donePath,new MyDonePathFilter(traceId));

	         if(doneFiles!=null&&doneFiles.length>0)
	        	 return true;
 
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		
			return false;
	}
	
	public void refreshSingleLogFolder(String projectFolder,String traceId) {
	//	String rootPath="C:\\Resources\\我的项目\\PhD\\博士论文\\大数据算法测试\\similarity\\icd-cac-sim-projects\\cac_count";
		String rootPath=projectFolder;
		String logPath=rootPath+"\\history";
		
		HadoopLog hl=new HadoopLog(this.env);
		
		TuningLog tlog=new TuningLog(rootPath);
		
		String single_log_path=logPath+"\\log-"+ traceId;
		
		hl.downloadDoneIntermediateJob(single_log_path, traceId);
		hl.downloadDoneJob(single_log_path, traceId);
		
		long timeCost=tlog.getTimeCost(traceId);
		
		System.out.println("traceId="+traceId+", timeCost="+timeCost);
	
		
		TuningLog tl = new TuningLog(projectFolder, null);
		tl.exportToCSV(new File(projectFolder).getName());
		
	}
	
	public void refreshHistoryFolder(String projectFolder) {
	//	String rootPath="C:\\Resources\\我的项目\\PhD\\博士论文\\大数据算法测试\\similarity\\icd-cac-sim-projects\\cac_count";
		String rootPath=projectFolder;
		String logPath=rootPath+"\\history";
		
		HadoopLog hl=new HadoopLog(this.env);
		
		
		List<String> traceIds=new ArrayList<String>();
		File[] logFolder = new File(logPath).listFiles();
		for(int i=0;i<logFolder.length;i++) {
			if(logFolder[i].isDirectory() && logFolder[i].getName().startsWith("log-")) {
				String[] ls=logFolder[i].getName().split("-");
				traceIds.add(ls[1]);
			}
		}
		
		TuningLog tlog=new TuningLog(rootPath);
		
		
		for(int i=0;i<traceIds.size();i++) {
			String traceId=traceIds.get(i);
		String single_log_path=logPath+"\\log-"+ traceId;
		
		hl.downloadDoneIntermediateJob(single_log_path, traceId);
		hl.downloadDoneJob(single_log_path, traceId);
		
		long timeCost=tlog.getTimeCost(traceId);
		
		System.out.println("traceId="+traceId+", timeCost="+timeCost);
		
		}
	
		TuningLog tl = new TuningLog(projectFolder, null);
		tl.exportToCSV(new File(projectFolder).getName());
		
	}
	
	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}
	
	public static void main(String[] args) {
		
		if(args.length==0) {
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost("192.168.1.72");
		he.setMasterPassword("mssmss");
		he.setMasterPort(22); 
		he.setMasterUser("hadoop"); 
		he.setHadoopBin("/usr/hadoop/bin/hadoop");
		he.setAppRoot("/usr/hadoop_apps");
		
		String rootPath="C:\\Resources\\我的项目\\PhD\\博士论文\\大数据算法测试\\similarity\\icd-cac-sim-projects\\cac_count";
		
		HadoopLog hl=new HadoopLog(he);
		hl.refreshHistoryFolder(rootPath);
		
		return;
		}
		
		Map<String, String> options = getOptionMap(args);
		HadoopLog hl=new HadoopLog(options.get("-dir"));
		hl.refreshHistoryFolder(options.get("-dir"));
		
		/*
		String traceId="1575516578852";
		String logPath=rootPath+"\\history";
		logPath+="\\log-"+traceId;
		
		hl.downloadDoneIntermediateJob(logPath, traceId);
		hl.downloadDoneJob(logPath, traceId);
		
		TuningLog tlog=new TuningLog(rootPath);
		
		long timeCost=tlog.getTimeCost(traceId);
		
		System.out.println("traceId="+traceId+", timeCost="+timeCost);
		*/

	}
	
}
