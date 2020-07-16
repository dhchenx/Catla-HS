package cn.edu.bjtu.cdh.catla.task;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cn.edu.bjtu.cdh.catla.task.HadoopLog.MyDonePathFilter;
import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;

public class SparkLog {

	private HadoopEnv env;
	
	public SparkLog(HadoopEnv env) {
		this.env=env;
	}

	
	private String donePath="/spark-job-history";
	
	private long timeAppCost;
	private long timeJobCost;
	
	public String getDonePath() {
		return donePath;
	}

	public void setDonePath(String donePath) {
		this.donePath = donePath;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getJobResult() {
		return jobResult;
	}

	public void setJobResult(String jobResult) {
		this.jobResult = jobResult;
	}

	public List<String> events=new ArrayList<String>();
	public List<Long> times=new ArrayList<Long>();
	
	private String appName;
	private String appId;
	private String jobResult;
	
	public void extractByHDFS(String logId,String outputFolder) {
		String hdfsPath=this.getDonePath()+"/"+logId;
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
		 FileSystem fs = FileSystem.get(conf);
		
		 String localPath= outputFolder+"/"+new File(hdfsPath).getName();
		
		this.writeFromHDFS2Local(fs, hdfsPath,localPath);
		List<String> lines = CatlaFileUtils
				.readFileByLine(localPath);
		process(lines);
		}catch(Exception ex) {
			ex.printStackTrace();
		}
    	
	}
	
	public void analyzeLog(String localPath) {
	
		try {
		
		List<String> lines = CatlaFileUtils
				.readFileByLine(localPath);
		process(lines);
		}catch(Exception ex) {
			ex.printStackTrace();
		}
    	
	}
	
	public void downloadSparkLog2Local(String appId,String localLogFolder) {
		String hdfsPath=this.getDonePath()+"/"+appId;
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
		 FileSystem fs = FileSystem.get(conf);
		
		this.writeFromHDFS2Local(fs, hdfsPath,localLogFolder+"/"+appId);
		List<String> lines = CatlaFileUtils
				.readFileByLine(localLogFolder+"/"+appId);
		process(lines);
		
		// time cost
		
		
		
		writeFile(localLogFolder+"/cost_"+this.getTimeAppCost(), this.getTimeAppCost()+"");
		
		}catch(Exception ex) {
			ex.printStackTrace();
		}
    	
	}
	
	public void downloadSparkLog2Local_Indiv(String appId,String localLogFolder) {
		String hdfsPath=this.getDonePath()+"/"+appId;
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
		 FileSystem fs = FileSystem.get(conf);
		
		this.writeFromHDFS2Local(fs,  "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort()+hdfsPath,localLogFolder+"/"+appId);
		List<String> lines = CatlaFileUtils
				.readFileByLine(localLogFolder+"/"+appId);
		process(lines);
		
		// time cost
		
		
		
		writeFile(localLogFolder+"/cost_"+this.getTimeAppCost(), this.getTimeAppCost()+"");
		
		}catch(Exception ex) {
			ex.printStackTrace();
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
	
	public String obtainAppId(String traceId) {
	
		try {
		//obtain all files
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.env.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.env.getMasterHost() + ":" + this.env.getHdfsPort());
		FileSystem fs = FileSystem.get(conf);
		
		
		//System.out.println("successFlagFolder = " +this.get);

			
		if(fs.exists(new org.apache.hadoop.fs.Path(this.getDonePath() + "/spark-id-" + traceId))) {
			
			org.apache.hadoop.fs.Path pt=new org.apache.hadoop.fs.Path(this.getDonePath() + "/spark-id-" + traceId);
			 
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			 String appId="";
			try {
			 
			  appId=br.readLine().trim();
			   
			} finally {
			  // you should close out the BufferedReader
			  br.close();
			}
			appId=appId.replace("\n","").trim();
			return appId;
		}else
			return null;
		}catch(Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	private void process(List<String> lines) {
		long timeAppStart = -1;
		long timeAppEnd = -1;
		long blockMAdded=-1;
		
		events=new ArrayList<String>();
		times=new ArrayList<Long>();
		long timeJobStart = -1;
		long timeJobEnd = -1;
		
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			System.out.println(line);
			JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();
			String event = jsonObject.get("Event").getAsString();
			//System.out.println("event: " + event);
			if (event.equals("SparkListenerApplicationStart")) {
				timeAppStart = jsonObject.get("Timestamp").getAsLong();
				appName = jsonObject.get("App Name").getAsString();
				appId = jsonObject.get("App ID").getAsString();
			}
	
			if (event.equals("SparkListenerApplicationEnd"))
				timeAppEnd = jsonObject.get("Timestamp").getAsLong();
			
			if (event.equals("SparkListenerJobStart"))
				timeJobStart = jsonObject.get("Submission Time").getAsLong();
			if (event.equals("SparkListenerJobEnd")) {
				timeJobEnd = jsonObject.get("Completion Time").getAsLong();
				jobResult = jsonObject.get("Job Result").getAsJsonObject().get("Result").getAsString();
				
			}
			
			
			if(jsonObject.has("Timestamp")) {
				long timeStamp=jsonObject.get("Timestamp").getAsLong();
				events.add(event);
				times.add(timeStamp);
			}
			
		}
		
		
		 setTimeAppCost(timeAppEnd - timeAppStart);
		 
		 setTimeJobCost(timeJobEnd-timeJobStart);
	}
	
	public void extractByLocal(String lpath) {
		List<String> lines = CatlaFileUtils
				.readFileByLine(lpath);
		process(lines);
		
	}
	
	public boolean checkExistsDoneLog( String traceId) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + env.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + env.getMasterHost() + ":" + env.getHdfsPort());
			  FileSystem hdfs = FileSystem.get(conf);
			  
			  org.apache.hadoop.fs.Path donePath = new org.apache.hadoop.fs.Path(this.donePath +"/*");

	         FileStatus[] doneFiles = hdfs.globStatus(donePath,new MyDonePathFilter(traceId));

	         if(doneFiles!=null&&doneFiles.length>0)
	        	 return true;
 
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		
			return false;
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
	
	public static void main(String[] args) {
		//
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost("192.168.100.105");
		he.setMasterPassword("123456"); 
		he.setMasterPort(22); 
		he.setMasterUser("hadoop");
		he.setHadoopBin("/home/bigdata/app/spark/bin/spark-submit");
		he.setAppRoot("/usr/spark_apps");
		he.setSparkUrl("spark://master:7077");
		
		//
		SparkLog sparkLog=new SparkLog(he);
		sparkLog.setDonePath("/spark-job-history");
		//sparkLog.extractByLocal("C:/Users/douglaschan/Desktop/spark/app-20191016015640-0002");
		sparkLog.extractByHDFS("app-20200716095919-0001", "C:/Users/douglaschan/Desktop/spark/tmp");
		
		System.out.println("app Name: "+ sparkLog.getAppName());
		System.out.println("app Id: "+ sparkLog.getAppId());
		
		DecimalFormat df = new DecimalFormat("#.00");
			
		System.out.println("time cost of app: "+ df.format(sparkLog.getTimeAppCost() * 1.0 /1000/60) +" mins");
			
		/*
		long minTime=Long.MAX_VALUE;
		long maxTime=-1;
		for(int i=0;i<sparkLog.events.size();i++) {
			System.out.println(sparkLog.events.get(i)+"\t"+sparkLog.times.get(i));
			if(sparkLog.times.get(i)<minTime)
				minTime=sparkLog.times.get(i);
			if(sparkLog.times.get(i)>maxTime)
				maxTime=sparkLog.times.get(i);
		}
		
		System.out.println("time cost of full job: "+ df.format((maxTime-minTime) * 1.0 /1000/60) +" mins");
		*/
		
		System.out.println("time cost of job: "+ df.format(sparkLog.getTimeAppCost() * 1.0 /1000/60) +" mins");
		System.out.println("time cost of job: "+ df.format(sparkLog.getTimeAppCost() * 1.0 /1000) +" secs");
		
		System.out.println("job result: "+ sparkLog.getJobResult());
		
	
	}


	public long getTimeAppCost() {
		return timeAppCost;
	}


	public void setTimeAppCost(long timeAppCost) {
		this.timeAppCost = timeAppCost;
	}

	public long getTimeJobCost() {
		return timeJobCost;
	}

	public void setTimeJobCost(long timeJobCost) {
		this.timeJobCost = timeJobCost;
	}

}
