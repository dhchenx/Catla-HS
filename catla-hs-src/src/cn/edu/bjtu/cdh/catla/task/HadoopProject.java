package cn.edu.bjtu.cdh.catla.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.logging.LogManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.edu.bjtu.cdh.catla.tuning.TuningLog;
import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;
import cn.edu.bjtu.cdh.catla.utils.UnicodeReader;


public class HadoopProject {
	private String id;
	private String description;
	private String inputs;
	private String outputs;
	private String jars;
	private String logs;
	
	private boolean updatelogs;

	public void saveToText(String path) {
		String line="";
		line+="Id"+" "+this.id+"\r\n";
		line+="Description"+" "+this.description+"\r\n";
		line+="Inputs"+" "+this.inputs+"\r\n";
		line+="Outputs"+" "+this.outputs+"\r\n";
		line+="Jars"+" "+this.jars+"\r\n";
		line+="Logs"+" "+this.logs;
		CatlaFileUtils.writeFile(path, line);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getInputs() {
		return inputs;
	}

	public void setInputs(String inputs) {
		this.inputs = inputs;
	}

	public String getOutputs() {
		return outputs;
	}

	public void setOutputs(String outputs) {
		this.outputs = outputs;
	}

	public String getJars() {
		return jars;
	}

	public void setJars(String jars) {
		this.jars = jars;
	}

	public String getLogs() {
		return logs;
	}

	public void setLogs(String logs) {
		this.logs = logs;
	}

	private List<Map<String, String>> envMaps = new ArrayList<Map<String, String>>();
	private List<Map<String, String>> uploadMaps = new ArrayList<Map<String, String>>();
	private List<Map<String, String>> submitMaps = new ArrayList<Map<String, String>>();

	private List<String> traceIds = new ArrayList<String>();

	
	private String rootFolder;

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

	public List<Map<String, String>> getEnvMaps() {
		return envMaps;
	}

	public void setEnvMaps(List<Map<String, String>> envMaps) {
		this.envMaps = envMaps;
	}

	public List<Map<String, String>> getUploadMaps() {
		return uploadMaps;
	}

	public void setUploadMaps(List<Map<String, String>> uploadMaps) {
		this.uploadMaps = uploadMaps;
	}

	public List<Map<String, String>> getSubmitMaps() {
		return submitMaps;
	}

	public void setSubmitMaps(List<Map<String, String>> submitMaps) {
		this.submitMaps = submitMaps;
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
		if(map.containsKey("AppType"))
			he.setAppType(map.get("AppType"));
		else 
			he.setAppType("Hadoop");
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
		
		if(map.containsKey("LogPath")) {
			ht.setLogPath(map.get("LogPath"));
		}
		
		if(map.containsKey("OtherArgs")) {
			ht.setOtherArgs(map.get("OtherArgs").split(" "));
		}
		
		if(!map.containsKey("Trace")) {
			ht.setTrace("false");
		}else {
			ht.setTrace(map.get("Trace"));
		}
		
		return ht;
	}

	public void callUploads() {
		callUploads("");
	}

	public void callUploads(String masterHost) {
		if (masterHost.isEmpty()) {
			callUploads(MapToEnv(this.getEnvMaps().get(0)));
			return;
		}

		for (int i = 0; i < this.getEnvMaps().size(); i++) {
			if (this.getEnvMaps().get(i).get("MasterHost").equals(masterHost)) {
				callUploads(MapToEnv(this.getEnvMaps().get(i)));
				return;
			}
		}
	}

	public void callUploads(HadoopEnv he) {

		HadoopApp ha = new HadoopApp(he);

		System.out.println("================Begin to upload jar(s) to server====================");

		System.out.println("upload task: " + this.getUploadMaps().size());

		// upload
		for (int i = 0; i < this.getUploadMaps().size(); i++) {

			HadoopJar hj = MapToJar(this.getUploadMaps().get(i));
			
			if(!new File(hj.getLocalJarPath()).exists()) {
				System.out.println("Jar does not exist! : "+hj.getLocalJarPath());
				continue;
			}

			boolean isUpload = ha.uploadJar(hj);
			System.out.println("Local file to upload:" + hj.getLocalJarPath());
			System.out.println("Remote path to upload:" + he.getAppRoot() + "/" + hj.getRemoteJarPath());
			System.out.println("Result of upload:" + isUpload);
		}

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

	private static int counter = 0;

	public boolean callSubmit() {
		return callSubmit("", false,false);
	}

	public boolean callSubmit(boolean needDownload, boolean waitForAllJobCompletion) {
		return callSubmit(MapToEnv(this.getEnvMaps().get(0)), needDownload,waitForAllJobCompletion);
	}
	
	public boolean callSubmit(boolean needDownload) {
		return callSubmit("", needDownload,false);
	}

	public boolean callSubmit(String masterHost, boolean needDownload, boolean waitForAllJobCompletion) {

		if (masterHost.isEmpty()) {
			return callSubmit(MapToEnv(this.getEnvMaps().get(0)), needDownload, waitForAllJobCompletion);
			 
		}
		int true_count=0;
		for (int i = 0; i < this.getEnvMaps().size(); i++) {
			if (this.getEnvMaps().get(i).get("MasterHost").equals(masterHost)) {
				
				boolean s= callSubmit(MapToEnv(this.getEnvMaps().get(i)), needDownload, waitForAllJobCompletion);
				 if(s)true_count++;
			}
		}
		
		if(true_count==this.getEnvMaps().size())
			return true;
		else
			return false;

	}

	private String getTimeStamp() {
		Date ss = new Date();

		// Date aw = Calendar.getInstance().getTime();//获得时间的另一种方式，测试效果一样
		SimpleDateFormat format0 = new SimpleDateFormat("yyyy-MM-dd HHmmss");
		String time = format0.format(ss.getTime());// 这个就是把时间戳经过处理得到期望格式的时间

		return time;

	}

	/**
	 * 递归删除目录下的所有文件及子目录下所有文件
	 * 
	 * @param dir 将要删除的文件目录
	 * @return boolean Returns "true" if all deletions were successful. If a
	 *         deletion fails, the method stops attempting to delete and returns
	 *         "false".
	 */
	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		
		return dir.delete();
	}
	
	private boolean useOldTraceId;
	
	private String oldTraceId;
	
	private String runningTraceId;
	
	public boolean callSubmit(HadoopEnv he, boolean needDownload) {
		return this.callSubmit(he, needDownload, false);
	}

	public boolean callSubmit(HadoopEnv he, boolean needDownload, boolean waitForAllJobCompletion) {
		if (he == null)
			he = MapToEnv(this.getEnvMaps().get(0));
		
		 
		
		System.out.println("waitForAllJobCompletion: "+waitForAllJobCompletion);

		//HadoopApp ha = new HadoopApp(he);
		
		final String appType=he.getAppType();
		
		IApp ha = AppFactory.createApp(he, appType);

		System.out.println("================Begin to submit tasks====================");
		System.out.println("task number: " + this.getSubmitMaps().size());
		for (int i = 0; i < this.getSubmitMaps().size(); i++) {

			final int currentI=i;
			
			HadoopTask ht = MapToTask(this.getSubmitMaps().get(i));
			
			String currentTraceId="";
			
			for(int j=0;j<ht.getArgs().length;j++) {
				if(ht.getArgs()[j].startsWith("@traceId=")) {
					currentTraceId=ht.getArgs()[j].split("=")[1];
				}
			}
			
			if(currentTraceId.isEmpty()) {
				if(!this.useOldTraceId)
					currentTraceId=""+System.currentTimeMillis();
				else
					currentTraceId=this.getOldTraceId();
				
				String[] args=ht.getArgs();
				String[] new_args=new String[args.length+1];
				for(int k=0;k<args.length;k++)
					new_args[k]=args[k];
				new_args[new_args.length-1]="@traceId="+currentTraceId;
				ht.setArgs(new_args);
				
			}
			
			if(appType.equals("Spark")) {
				String[] args=ht.getArgs();
			    String[] new_args=new String[args.length+2];
			    for (int x=0;x<args.length;x++) {
			    	new_args[x]=args[x];
			    }
			    new_args[args.length]="@masterIP=master";
			    new_args[args.length+1]="@historyUrl="+new SparkLog(he).getDonePath().replace("/", "");
				ht.setArgs(new_args);
			}
			
			this.getTraceIds().add(currentTraceId);
			
			if(ha.existInHDFS(ht.getFolderOfSuccessFlag())) {
				ha.deleteInHDFS(ht.getFolderOfSuccessFlag(), true);
			}
			
			this.setRunningTraceId(currentTraceId);
	
			
			String output_str = ha.submitTask(ht);
			
			if(!new File(this.getLogs()).exists())
				new File(this.getLogs()).mkdirs();

			writeFile(
					this.getLogs() + "/out-"
							+ this.getSubmitMaps().get(i).get("filename").substring(0,
									this.getSubmitMaps().get(i).get("filename").lastIndexOf("."))
							+ getTimeStamp(),
					output_str);


			if (needDownload&&ht.isAsync()) {
				
				System.out.println(
						"================Begin to detect if the task is finised when Async=True ====================");
				CountDownLatch  cdl = new CountDownLatch (1);
				
				Timer timer = new Timer();
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						
						// TODO Auto-generated method stub
						boolean success_flag = ha.isSuccess(ht);
						counter++;
						System.out.println("Detecting " + (counter) + " if it is finished: " + success_flag);
						if (success_flag) {
							this.cancel();
							System.out.println("Finished? => " + success_flag);
							
							if (ht.getOutputLocalRootFolder() != null && !ht.getOutputLocalRootFolder().isEmpty()) {

								System.out.println("Download... Async: " + ht.getOutputLocalRootFolder());
								ha.downloadResultToLocal(ht);
								
								if(waitForAllJobCompletion)
								cdl.countDown();
								
								
							}

						}
						
						
						
					}

				}, 5 * 1000, 2 * 1000);// delay, period
				
				 try {
					 
					 if(waitForAllJobCompletion)
				        cdl.await();
				       
				    } catch (InterruptedException ex) {
				    	ex.printStackTrace();
				    }

			} else  if (needDownload&&!ht.isAsync()){

				
				if (ha.isSuccess(ht)) {
					if (ht.getOutputLocalRootFolder() != null && !ht.getOutputLocalRootFolder().isEmpty()) {

						System.out.println("Download... Sync: " + ht.getOutputLocalRootFolder());
						ha.downloadResultToLocal(ht);
					}
				}

			}
			
			final String traceId=currentTraceId;
			
			if(ht.getTrace()!=null&& ht.getTrace().equals("true")) {
			
				counter=0;
				
				//Hadoop
				if(appType.equals("Hadoop")) {
					
					System.out.println(
							"================Begin to detect if the Hadoop task log is finised when Trace=true ====================");
					
				
				ht.setLogPath(this.getRootFolder()+"/history/log-"+currentTraceId);
				
				if(!new File(ht.getLogPath()).exists())
					new File(ht.getLogPath()).mkdirs();
				
				ht.saveToFile("submit", ht.getLogPath()+"/task_submit.txt");
				
				HadoopLog hlog=new HadoopLog(he);
				
				
				TuningLog tlog=new TuningLog(this.getRootFolder());
				
				
				CountDownLatch  cdl = new CountDownLatch (1);
				Timer timer = new Timer();
				timer.schedule(new TimerTask() {
					@Override
					public void run() {
						
						// TODO Auto-generated method stub
						boolean success_flag = hlog.checkExistsDoneIntermediateLog(traceId);
						boolean success_flag_done = hlog.checkExistsDoneLog(traceId);
						counter++;
						System.out.println("Detecting " + (counter) + " if the log finished: " + success_flag);
						if (success_flag) {
							//this.cancel();
							System.out.println("Finished Log? => " + success_flag);
						
							if (ht.getLogPath() != null && !ht.getLogPath().isEmpty()) {

								System.out.println("Download Log... Async: " + ht.getLogPath());
								hlog.downloadDoneIntermediateJob(ht.getLogPath(), traceId);
								
							//	if(waitForAllJobCompletion)
							//	cdl.countDown();
								
								
							}

						}
						
						if(success_flag_done) {
						//	this.cancel();
							System.out.println("Finished Done Log? => " + success_flag_done);
							// 6. 下载结果
							if (ht.getLogPath() != null && !ht.getLogPath().isEmpty()) {

								System.out.println("Download DoneLog... Async: " + ht.getLogPath());
								hlog.downloadDoneJob(ht.getLogPath(), traceId);
								
						
								
							//	if(waitForAllJobCompletion)
								//	cdl.countDown();
								
							}
						}
						
						if(success_flag||success_flag_done) {
							
							this.cancel();
							
							long timeCost=tlog.getTimeCost(traceId);
							
							System.out.println("total time cost = "+timeCost);
							
							
							if(waitForAllJobCompletion)
								cdl.countDown();
						}
					
					}

				}, 5 * 1000, 2 * 1000);// delay, period
			
				
				 try {
					 if(waitForAllJobCompletion)
				        cdl.await();

				    } catch (InterruptedException ex) {
				    	ex.printStackTrace();
				    	
				    }
				
			}
				
				if(appType.equals("Spark")) {
					
					
					System.out.println(
							"================Begin to detect if the Spark task log is finised when Trace=true ====================");
					
					counter=0;
				 
					ht.setLogPath(this.getRootFolder()+"/history/log-"+currentTraceId);
					
					if(!new File(ht.getLogPath()).exists())
						new File(ht.getLogPath()).mkdirs();
					
					ht.saveToFile("submit", ht.getLogPath()+"/task_submit.txt");
					
					SparkLog hlog=new SparkLog(he);
					
					CountDownLatch  cdl = new CountDownLatch (1);
					Timer timer = new Timer();
					timer.schedule(new TimerTask() {
						@Override
						public void run() {
							// TODO Auto-generated method stub
							
							boolean success_flag = ha.isSuccess(ht);
							
							counter++;
							System.out.println("Detecting " + (counter) + " if the Spark log is finished: " + success_flag);
							if (success_flag) {
								//this.cancel();
								System.out.println("Finished Log? => " + success_flag);
							
								if (ht.getLogPath() != null && !ht.getLogPath().isEmpty()) {

									String appId=((SparkApp)ha).getAppId();
									String logFolder=rootFolder+"/history/log-"+traceId;
									if(!new File(logFolder).exists()) {
										new File(logFolder).mkdirs();
									}
									
									hlog.downloadSparkLog2Local(appId, logFolder);
									
									System.out.println("Download Spark Log... Async: " + ht.getLogPath());
									
									writeFile(logFolder+"/iteration_"+(currentI+1),"");
									writeFile(logFolder+"/spark.app.id",appId);
									
								//	if(waitForAllJobCompletion)
								//	cdl.countDown();
									
									
								}

							}
							
						
							
								this.cancel();

								if(waitForAllJobCompletion)
									cdl.countDown();
							
						
						}

					}, 5 * 1000, 2 * 1000);// delay, period
				
					
					 try {
						 if(waitForAllJobCompletion)
					        cdl.await();

					    } catch (InterruptedException ex) {
					    	ex.printStackTrace();
					    	
					    }
					
				
					
					
				}
				
				
				
			}
			
			
			
			
			
			
		}
		
		
		if(this.isUpdatelogs()) {
			
			HadoopLog hl=new HadoopLog(he);
			hl.refreshHistoryFolder(this.getRootFolder());
			
		}
		
		System.out.println("finished "+this.getSubmitMaps().size()+" task(s). ");
		
		return true;
		
	}
	
	private List<File> filelist=null;
	
	private List<File> getFileList(String strPath) {
		File dir = new File(strPath);
		File[] files = dir.listFiles(); 
		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				String fileName = files[i].getName();
				if (files[i].isDirectory()) { 
					getFileList(files[i].getAbsolutePath());
				} else { 
					String strFileName = files[i].getAbsolutePath().replace("\\", "/");
					System.out.println("---" + strFileName);
					filelist.add(files[i]);
				} 
			}
 
		}
		return filelist;
	}
	

	public void callUpload2HDFS() {
		callUpload2HDFS("");
	}

	public void callUpload2HDFS(String masterHost) {

		if (masterHost.isEmpty()) {
			callUpload2HDFS(MapToEnv(this.getEnvMaps().get(0)));
			return;
		}

		for (int i = 0; i < this.getEnvMaps().size(); i++) {
			if (this.getEnvMaps().get(i).get("MasterHost").equals(masterHost)) {
 
				callUpload2HDFS(MapToEnv(this.getEnvMaps().get(i)));
				return;
			}
		}

	}
 
	public void callUpload2HDFS(HadoopEnv he) {
		if (he == null)
			he = MapToEnv(this.getEnvMaps().get(0));
		System.out.println("=====================Begin to upload files to HDFS================");
		if(this.getInputs()==null||this.getInputs().isEmpty()||!new File(this.getInputs()).exists())
		{
			System.out.println("directory: "+this.getInputs() +" is not available!");
			return;
		}
		
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + he.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + he.getMasterHost() + ":" + he.getHdfsPort());
		FileSystem fs = FileSystem.get(conf);

		
		this.setInputs(this.getInputs().replace("\\", "/"));
		System.out.println("inputs="+this.getInputs());
		filelist=new ArrayList<File>();
		filelist=getFileList(this.getInputs());
		System.out.println(this.getInputs());
		for(int i=0;i<filelist.size();i++) {
			String filename=filelist.get(i).getAbsolutePath().replace("\\","/");
			
			if(!new File(filename).exists())
				continue;
			
			String rel_path=filename.replace(this.getInputs(), "");
			System.out.println("copying files to the HDFS path: "+rel_path);

			
	        Path src = new Path(filename);
	       
	        Path dst = new Path(rel_path);
	        fs.copyFromLocalFile(src, dst);
	        
		}
		
	    fs.close();
		
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	public static HadoopProject createInstance(String project_folder) {

		return createInstance(project_folder,null);
	}

	public static HadoopProject createInstance(final String project_folder,Map<String,String> tuningParameters) {

		LogManager.getLogManager().reset();

		HadoopProject hp = new HadoopProject();

		System.out.println("Building project: " + project_folder);

		String proj_file_path = project_folder + "/" + "_hproj";
		
		if(!new File(proj_file_path).exists()) {
			proj_file_path = project_folder + "/" + "_hproj.txt";
		}
		
		if (!new File(proj_file_path).exists()) {
			System.out.println("the folder is not a suitable project folder, exited!");
			return null;
		}

		// create basic info of the project and their absolute paths
		System.out.println("==>" + proj_file_path);
		Map<String, String> proj_map = getMap(readFileByLines(proj_file_path));
		hp.setRootFolder(project_folder);
		hp.setDescription(proj_map.get("Description"));
		hp.setId(proj_map.get("Id"));
		hp.setInputs(hp.getRootFolder() + "/" + proj_map.get("Inputs"));
		hp.setOutputs(hp.getRootFolder() + "/" + proj_map.get("Outputs"));
		hp.setJars(hp.getRootFolder() + "/" + proj_map.get("Jars"));
		hp.setLogs(hp.getRootFolder() + "/" + proj_map.get("Logs"));

		// identify possible tasks' map data
		File file = new File(hp.getRootFolder());
		File[] tfiles = file.listFiles();

		for (int i = 0; i < tfiles.length; i++) {

			System.out.println(tfiles[i].getName() + " ==> ");

			if (!tfiles[i].isFile())
				continue;

			if (!tfiles[i].getName().startsWith("task_") && !tfiles[i].getName().startsWith("env_"))
				continue;

			Map<String, String> map = getMap(readFileByLines(tfiles[i].getAbsolutePath()));

			String type = map.get("Task");
			if (type != null) {

				map.put("filename", tfiles[i].getName());

				if (type.equals("init"))
					hp.getEnvMaps().add(map);
				if (type.equals("upload"))
					hp.getUploadMaps().add(map);
				if (type.equals("submit"))
					hp.getSubmitMaps().add(map);

			}
		}

		for (int i = 0; i < hp.getSubmitMaps().size(); i++) {
			
			
			if (hp.getSubmitMaps().get(i).containsKey("OutputLocalRootFolder")
					&& (!hp.getSubmitMaps().get(i).get("OutputLocalRootFolder").startsWith("/")
							&& !hp.getSubmitMaps().get(i).get("OutputLocalRootFolder").contains(":"))) {
				hp.getSubmitMaps().get(i).put("OutputLocalRootFolder",
						hp.getRootFolder() + "/" + hp.getSubmitMaps().get(i).get("OutputLocalRootFolder"));

			}
			
			if (hp.getSubmitMaps().get(i).containsKey("LogPath")
					&& (!hp.getSubmitMaps().get(i).get("LogPath").startsWith("/")
							&& !hp.getSubmitMaps().get(i).get("LogPath").contains(":"))) {
				hp.getSubmitMaps().get(i).put("LogPath",
						hp.getRootFolder() + "/" + hp.getSubmitMaps().get(i).get("LogPath"));

			}
			
			//inject tuning paramters
			if(tuningParameters!=null) {
				 for (Map.Entry<String,String> entry : tuningParameters.entrySet())  
				 {
					 hp.getSubmitMaps().get(i).put(entry.getKey(), entry.getValue());
				 }
			}

		}

		for (int i = 0; i < hp.getUploadMaps().size(); i++) {
			if (hp.getUploadMaps().get(i).containsKey("LocalJarPath")
					&& (!hp.getUploadMaps().get(i).get("LocalJarPath").startsWith("/")
							&& !hp.getUploadMaps().get(i).get("LocalJarPath").contains(":"))) {
				hp.getUploadMaps().get(i).put("LocalJarPath",
						hp.getRootFolder() + "/" + hp.getUploadMaps().get(i).get("LocalJarPath"));

			}
		}

		return hp;

	}

	public String getRootFolder() {
		return rootFolder;
	}
	

	public void setRootFolder(String rootFolder) {
		this.rootFolder = rootFolder;
	}

	public static Map<String, String> getOptionMap(String[] args) {

		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < args.length - 1; i = i + 2) {
			map.put(args[i], args[i + 1]);
		}
		return map;
	}

	public static void main(String[] args) throws Exception {
		
		URL rootUrl=HadoopProject.class.getProtectionDomain().getCodeSource().getLocation();
		String jarFolder=URLDecoder.decode(rootUrl.getPath(), "utf-8");
		
		if(jarFolder.endsWith(".jar")) {
			jarFolder=jarFolder.substring(0,jarFolder.lastIndexOf("/"));
			if(jarFolder.contains(":"))
				jarFolder=jarFolder.substring(1);
			
		}else {
			jarFolder="";
		}
		
		
		
		/*
		args=new String[] {
			"-dir"	,"C:\\Resources\\我的项目\\PhD\\博士论文\\data\\门诊数据\\hproj_mapjoin",
			"-task", "submit",
			"-download", "true"
			
		};
		*/
		
		System.out.println("A Simple Script Tool for Hadoop Jobs designed by Donghua Chen!");

		if (args.length == 0) {
			System.out.println("Help");
			System.out.println("-dir (required)" + "\t" + "The path of project folder that contains '_hproj.txt' file");
			System.out.println(
					"-download (optional=false)" + "\t" + "Download the results when the task is finished! value: true or false");
			System.out.println(
					"-sequence (optional=false)" + "\t" + "Determine if submitting at the same time or in a sequence for mutiple jobs. value: true (in Sequence) or false");
			System.out.println("-task (required)" + "\t" + "Some options: uploadjar, submit, uploadhdfs, pipeline");
			System.out.println("-master (optional=the first env_ file)" + "\t" + "Specify the target hadoop environment file when there are mutiple env_* file in the folder");
			System.out.println("Example: "
					+ "java HadoopProject.jar -dir C:/usr/local/wordcount -task pipeline -download true -sequence true");
			return;
		}

		Map<String, String> options = getOptionMap(args);

		if (!options.containsKey("-dir")) {
			System.out.println("no directory pointed! please specific -dir parameter!");

			return;
		}
		
		if(!jarFolder.isEmpty()) {
			if(!options.get("-dir").contains(":")&&!options.get("-dir").startsWith("/")){
				options.put("-dir", jarFolder+"/"+options.get("-dir"));
			}
			}

	

		boolean is_download = false;
		if (options.containsKey("-download")) {
			String need_download = options.get("-download");
			if (need_download != null && need_download.equalsIgnoreCase("true"))
				is_download = true;
		}

		String ip = "";
		if (options.containsKey("-master")) {
			ip = options.get("-master");
		}
		
		boolean is_sequence=false;
		if(options.containsKey("-sequence"))
			is_sequence=Boolean.parseBoolean(options.get("-sequence"));
		
		boolean is_updatelogs=false;
		if(options.containsKey("-updatelogs"))
			is_updatelogs=Boolean.parseBoolean(options.get("-updatelogs"));

		//create a project
		HadoopProject hp = HadoopProject.createInstance(options.get("-dir"));
		
		hp.setUpdatelogs(is_updatelogs);

		if (options.containsKey("-task")) {
			String op = options.get("-task");
			if (op.equalsIgnoreCase("uploadJar")) {
				hp.callUploads(ip);
			}else
			if (op.equalsIgnoreCase("submit")) {
				hp.callSubmit(ip, is_download,is_sequence);
			}else if(op.equalsIgnoreCase("uploadhdfs")) {
				hp.callUpload2HDFS(ip);
			}else if(op.equalsIgnoreCase("pipeline")) {
				System.out.println("Using pipeline mode means the ouput files will automatically be downloaded!");
				hp.callUpload2HDFS(ip); //Step 1
				hp.callUploads(ip); //Step 2
				hp.callSubmit(ip, is_download,is_sequence); //Step 3
			}else {
				System.out.println("-task parameter is wrong specified!");
			}
		} else {
			System.out.println("no -task is specified!");
		}

	}

	public List<String> getTraceIds() {
		return traceIds;
	}

	public void setTraceIds(List<String> traceIds) {
		this.traceIds = traceIds;
	}

	public String getRunningTraceId() {
		return runningTraceId;
	}

	public void setRunningTraceId(String runningTraceId) {
		this.runningTraceId = runningTraceId;
	}

	public String getOldTraceId() {
		return oldTraceId;
	}

	public void setOldTraceId(String oldTraceId) {
		this.oldTraceId = oldTraceId;
	}

	public boolean isUseOldTraceId() {
		return useOldTraceId;
	}

	public void setUseOldTraceId(boolean useOldTraceId) {
		this.useOldTraceId = useOldTraceId;
	}

	public boolean isUpdatelogs() {
		return updatelogs;
	}

	public void setUpdatelogs(boolean updatelogs) {
		this.updatelogs = updatelogs;
	}

}
