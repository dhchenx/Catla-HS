package cn.edu.bjtu.cdh.catla.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.LogManager;

import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;
import cn.edu.bjtu.cdh.catla.utils.UnicodeReader;

public class HadoopTask {
	private String jarLocalPath;
	private String jarRemotePath;
	private String logPath;
	
	private String trace;
	
	private String[] otherArgs;
	
 
	public String[] getOtherArgs() {
		return otherArgs;
	}
	public void setOtherArgs(String[] otherArgs) {
		this.otherArgs = otherArgs;
	}
	public String getLogPath() {
		return logPath;
	}
	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}
	public String getJarLocalPath() {
		return jarLocalPath;
	}
	public void setJarLocalPath(String jarLocalPath) {
		this.jarLocalPath = jarLocalPath;
	}
	public String getJarRemotePath() {
		return jarRemotePath;
	}
	public void setJarRemotePath(String jarRemotePath) {
		this.jarRemotePath = jarRemotePath;
	}
	public String getMainClass() {
		return mainClass;
	}
	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}
	public String[] getArgs() {
		return args;
	}
	public String getFolderOfSuccessFlag() {
		return folderOfSuccessFlag;
	}
	public void setFolderOfSuccessFlag(String folderOfSuccessFlag) {
		this.folderOfSuccessFlag = folderOfSuccessFlag;
	}


	public void setArgs(String[] args) {
		this.args = args;
	}
	private String mainClass;
	private String[] args;
	private boolean Async;
	public boolean isAsync() {
		return Async;
	}
	public String[] getOutputHdfsFolders() {
		return outputHdfsFolders;
	}
	public void setOutputHdfsFolders(String[] outputHdfsFolders) {
		this.outputHdfsFolders = outputHdfsFolders;
	}

	public String getOutputLocalRootFolder() {
		return outputLocalRootFolder;
	}
	public void setOutputLocalRootFolder(String outputLocalRootFolder) {
		this.outputLocalRootFolder = outputLocalRootFolder;
	}
	public void setAsync(boolean async) {
		Async = async;
	}
	
	private String folderOfSuccessFlag;
	
	private String[] outputHdfsFolders;

	private String outputLocalRootFolder;
	
	public void saveToFile(String task,String path) {
		String args_str="";
		
		if(args!=null)
		for(int i=0;i<args.length;i++) {
			if(i!=args.length-1)
				args_str+=args[i]+" ";
			else
				args_str+=args[i];
		}
		String other_args_str="";
		if(otherArgs!=null)
			for(int i=0;i<otherArgs.length;i++) {
				if(i!=otherArgs.length-1)
					other_args_str+=otherArgs[i]+" ";
				else
					other_args_str+=otherArgs[i];
			}
		
		String out_str="";
		if(this.getOutputHdfsFolders()!=null)
		for(int i=0;i<this.getOutputHdfsFolders().length;i++) {
			if(i!=this.getOutputHdfsFolders().length-1)
				out_str+=this.getOutputHdfsFolders()[i]+" ";
			else
				out_str+=this.getOutputHdfsFolders()[i];
		}
		
		String line="";
		line+="Task"+" "+task+"\r\n";
		line+="Async"+" "+this.Async+"\r\n";
		line+="JarRemotePath"+" "+this.jarRemotePath+"\r\n";
		line+="MainClass"+" "+this.mainClass+"\r\n";
		line+="Args"+" "+args_str+"\r\n";
		line+="OtherArgs"+" "+other_args_str+"\r\n";
		line+="FolderOfSuccessFlag"+" "+this.folderOfSuccessFlag+"\r\n";
		line+="OutputHdfsFolders"+" "+out_str+"\r\n";
		line+="OutputLocalRootFolder"+" "+this.outputLocalRootFolder+"\r\n";
		line+="Trace"+" "+this.trace+"\r\n";
		line+="LogPath"+" "+this.logPath;
		CatlaFileUtils.writeFile(path, line);
	}
	public String getTrace() {
		return trace;
	}
	public void setTrace(String trace) {
		this.trace = trace;
	}
	
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
		return he;
	}

	public static HadoopJar MapToJar(Map<String, String> map) {
		HadoopJar hj = new HadoopJar();
		hj.setLocalJarPath(map.get("LocalJarPath"));
		hj.setRemoteJarPath(map.get("RemoteJarPath"));
		hj.setDeleteExistingFile(Boolean.parseBoolean(map.get("DeleteExistingFile")));

		if(!hj.getLocalJarPath().startsWith("/")&&!hj.getLocalJarPath().contains(":")) {
			hj.setLocalJarPath(dirFolder+"/"+hj.getLocalJarPath());
		}
		
		return hj;
	}

	public static HadoopTask MapToTask(Map<String, String> map) {
		HadoopTask ht = new HadoopTask();
		ht.setAsync(Boolean.parseBoolean(map.get("Async")));
		ht.setJarRemotePath(map.get("JarRemotePath"));
		ht.setMainClass(map.get("MainClass"));

		ht.setArgs(map.get("Args").split(" "));
		ht.setFolderOfSuccessFlag(map.get("FolderOfSuccessFlag"));
		ht.setOutputHdfsFolders(map.get("OutputHdfsFolders").split(" "));

		ht.setOutputLocalRootFolder(map.get("OutputLocalRootFolder"));
		
		if(!ht.getOutputLocalRootFolder().startsWith("/")&&!ht.getOutputLocalRootFolder().contains(":")) {
			ht.setOutputLocalRootFolder( dirFolder+ "/"+ ht.getOutputLocalRootFolder());
		}
		
		if (map.containsKey("OtherArgs"))
			ht.setOtherArgs(map.get("OtherArgs").split(" "));
		
		return ht;
	}

	public static List<String> readFileByLines(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		List<String> lines = new ArrayList<String>();
		try {

			UnicodeReader read = new UnicodeReader(new FileInputStream(file),
					"UTF-8");
			
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

	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}
	
	static  int counter=0;
	
	public static String jarFolder="";
	public static String dirFolder="";

	public static void main(String[] args) throws Exception {
		System.out.println("A Hadoop Task Manager designed by Donghua Chen");
		
		URL rootUrl=HadoopTask.class.getProtectionDomain().getCodeSource().getLocation();
		String jarFolder=URLDecoder.decode(rootUrl.getPath(), "utf-8");
		
		if(jarFolder.endsWith(".jar")) {
			jarFolder=jarFolder.substring(0,jarFolder.lastIndexOf("/"));
			if(jarFolder.contains(":"))
				jarFolder=jarFolder.substring(1);
			
		}else {
			jarFolder="";
		}
		
		System.out.println("jarpath="+ jarFolder);
		
		//tring path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath() 
		
		LogManager.getLogManager().reset();

		// HadoopTaskRun -ef HadoopEnv.txt -tf Task_MapJoin.txt
		if(args.length==0) {
			args = new String[] { "-dir", "C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\task模式示例\\task_wordcount" };
		}

		Map<String, String> options = getOptionMap(args);

		if(options.containsKey("-p")&&options.get("-p").equals("version")) {
		 System.out.println("version 1.0");
		}

		
		if (!options.containsKey("-dir")) {
			System.out.println("no directory pointed! please specific -dir parameter!");
			return;
		}
		
		if(!jarFolder.isEmpty()) {
		if(!options.get("-dir").contains(":")&&!options.get("-dir").startsWith("/")){
			options.put("-dir", jarFolder+"/"+options.get("-dir"));
		}
		}
		
		System.out.println("-dir:"+options.get("-dir"));

		
		File file = new File(options.get("-dir"));
		// get the folder list
		File[] tfiles = file.listFiles();
		List<Map<String, String>> env_files = new ArrayList<Map<String, String>>();
		List<Map<String, String>> upload_files = new ArrayList<Map<String, String>>();
		List<Map<String, String>> submit_files = new ArrayList<Map<String, String>>();
		
	    dirFolder=options.get("-dir");
	    final String rootFolder=dirFolder;

		for (int i = 0; i < tfiles.length; i++) {
			System.out.println(tfiles[i].getName()+" ==> ");
			if(!tfiles[i].isFile())
				continue;
			if(!tfiles[i].getName().endsWith(".txt"))
				continue;
			Map<String, String> map = getMap(readFileByLines(tfiles[i].getAbsolutePath()));
			String type = map.get("Task");
			if (type != null) {
				if (type.equals("upload"))
					upload_files.add(map);
				if (type.equals("init"))
					env_files.add(map);
				if (type.equals("submit"))
					submit_files.add(map);
			}
		}
		
		HadoopEnv he = null;
		if (env_files.size() > 1)
			throw new Exception("Number of Hadoop env files should be no more than 1.");
		else {
			he = MapToEnv(env_files.get(0));
		}

		
		String appName="Hadoop";
		if(he.getSparkUrl()!=null&&!he.getSparkUrl().equals("")) {
			appName="Spark";
		}
		
		IApp ha = AppFactory.createApp(he, appName);

		System.out.println("================Begin to upload jar(s) to server====================");
		
		
		// upload
		for (int i = 0; i < upload_files.size(); i++) {

			HadoopJar hj = MapToJar(upload_files.get(i));

			boolean isUpload = ha.uploadJar(hj);
			System.out.println("Local path: " + hj.getLocalJarPath());
			System.out.println("Remote Path: " + he.getAppRoot()+"/"+ hj.getRemoteJarPath());
			System.out.println("Uploading Result: " + isUpload);
		}

		System.out.println("================Begin to submit tasks====================");
		
		final String myAppName=appName;
		final SparkLog sparkLog=new SparkLog(he);;
		
	
			
		

		for (int i = 0; i < submit_files.size(); i++) {

			HadoopTask ht = MapToTask(submit_files.get(i));

			// 4. 提交任务
			String output_str = ha.submitTask(ht);
			
			
			// 5. 定时检测任务是否完成
			System.out.println("================ Detecting if the task is finished...====================");
			if (ht.isAsync()) {
				
				 
				Timer timer = new Timer();
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						
						// TODO Auto-generated method stub
						boolean success_flag = ha.isSuccess(ht);
						counter++;
						System.out.println("Detecting "+(counter)+" if finished!: " + success_flag);
						if (success_flag) {
							this.cancel();
							System.out.println("Finished: " + success_flag);
						
							if (ht.getOutputLocalRootFolder() != null && !ht.getOutputLocalRootFolder().isEmpty()) {
								System.out.println("Donwloaded to (Async): " + ht.getOutputLocalRootFolder());
								ha.downloadResultToLocal(ht);
							}
							
							//spark log download
							if (myAppName.equals("Spark")) {
								String logId=((SparkApp)ha).getAppId();
								String logFolder=rootFolder+"/logs";
								if(!new File(logFolder).exists()) {
									new File(logFolder).mkdir();
								}
								
								sparkLog.extractByHDFS(logId,logFolder);
								
							}

							
						}
					}

				}, 10 * 1000, 5 * 1000);// delay, period

			} else {

			
				if (ha.isSuccess(ht)) {
					if (ht.getOutputLocalRootFolder() != null && !ht.getOutputLocalRootFolder().isEmpty()) {
						System.out.println("Downloaded to (Sync): " + ht.getOutputLocalRootFolder());
						ha.downloadResultToLocal(ht);
					}
					

					//spark log download
					if (myAppName.equals("Spark")) {
						String logId=((SparkApp)ha).getAppId();
						String logFolder=rootFolder+"/logs";
						if(!new File(logFolder).exists()) {
							new File(logFolder).mkdir();
						}
						
						sparkLog.extractByHDFS(logId,logFolder);
						
					}
				}

			}
			
			System.out.println(output_str);
		}

	}
	
	
	
}
