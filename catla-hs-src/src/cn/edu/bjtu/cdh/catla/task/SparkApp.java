package cn.edu.bjtu.cdh.catla.task;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.LogManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.future.AuthFuture;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.SftpClientFactory;
import org.apache.sshd.client.subsystem.sftp.SftpClient.DirEntry;

public class SparkApp implements IApp {
	
	
	private HadoopEnv hadoopEnv;

	public SparkApp(HadoopEnv he) {
		this.hadoopEnv = he;
	}
	
	private String allOutputResult;
	
	 
	public String executeTask(HadoopTask ht) {
		SshClient client = SshClient.setUpDefaultClient();
		//spark-submit --class org.apache.spark.examples.JavaWordCount 
		//--master spark://master:7077 --executor-memory 500m --total-executor-cores 3 /usr/spark_apps/SparkTest.jar hdfs://master:9000/data/cdh/examples/wordcount/input
	
		try {
			
			int other_len=0;
			if(ht.getOtherArgs()!=null)
				 other_len=ht.getOtherArgs().length;

			String[] hadoopCMD = new String[4 + ht.getArgs().length+other_len];
			hadoopCMD[0] = this.hadoopEnv.getHadoopBin();
			hadoopCMD[1] =  "--class "+ ht.getMainClass();
			hadoopCMD[2] = "--master "+ this.hadoopEnv.getSparkUrl();

			
			if(other_len!=0) {
				for(int i=0;i<ht.getOtherArgs().length;i++) {
					hadoopCMD[3+i]=ht.getOtherArgs()[i];
				}
			}
			

			hadoopCMD[3+other_len] = ht.getJarRemotePath();
			
			if (ht.getArgs() != null)
				for (int i = 0; i < ht.getArgs().length; i++)
					hadoopCMD[other_len+i + 4] = ht.getArgs()[i];

			
			String hadoopCMDStr = "";
			for (int i = 0; i < hadoopCMD.length; i++) {
				if (i != hadoopCMD.length - 1)
					hadoopCMDStr += hadoopCMD[i] + " ";
				else
					hadoopCMDStr += hadoopCMD[i];
			}
			
		

			System.out.println("sparkCmdStr = " + hadoopCMDStr);

			String cmd = hadoopCMDStr;
			client = SshClient.setUpDefaultClient();
			client.start();
			ConnectFuture connect = client.connect(this.hadoopEnv.getMasterUser(), this.hadoopEnv.getMasterHost(),
					this.hadoopEnv.getMasterPort());
			boolean issuccess = connect.await(5000L);
			if (issuccess) {
				ClientSession session = connect.getSession();
				session.addPasswordIdentity(this.hadoopEnv.getMasterPassword());
				// session.addPublicKeyIdentity(SecurityUtils.loadKeyPairIdentity("keyname", new
				// FileInputStream("priKey.pem"), null));

				AuthFuture auth = session.auth();
				auth.await();
				if (!auth.isSuccess())
					System.out.println("Authenication failed!");

				ChannelExec ec = session.createExecChannel(cmd);

				ByteArrayOutputStream errs = new ByteArrayOutputStream();

				ByteArrayOutputStream outs = new ByteArrayOutputStream();

				ec.setOut(outs);
				ec.setErr(errs);
				

				ec.open();

				// ec.wait(1000L);
				// ec.waitFor(Arrays.asList(ClientChannelEvent.CLOSED), 0);
				if (!ht.isAsync())
					ec.waitFor(Arrays.asList(ClientChannelEvent.CLOSED), 0);
				else
					ec.waitFor(Arrays.asList(ClientChannelEvent.OPENED), 0);
				// ec.wait(1000L);

				ec.close();
				client.stop();

				String result = new String(outs.toByteArray(), "utf-8");
				
				String err_result = new String(errs.toByteArray(), "utf-8");

				// System.out.println("results=");
				// System.out.println(result);
				if (!err_result.isEmpty()) {
					System.out.println("error information as below:");
					System.out.println(err_result);
				}
				
				if(result!=null&&!result.isEmpty()) {
					for(String line:result.split("\n")) {
						if(line.startsWith("TIMECOST")||line.startsWith("程序运行时间")) {
							System.out.println(line);
							break;
						}
					}
				}
				
				allOutputResult=result+"\n"+err_result;
					
				return result;

			} else {
				if (ht.isAsync())
					return "Async";
				return "";
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			if (client != null & client.isOpen())
				try {
					client.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (ht.isAsync())
				return "Async";
			return "";
		}

	}
	
	public boolean deleteInHDFS(String hdfsPath,boolean isRecursive) {
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.hadoopEnv.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.hadoopEnv.getMasterHost() + ":" + this.hadoopEnv.getHdfsPort());
		  FileSystem hdfs = FileSystem.get(conf);
		  org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(hdfsPath);
		  boolean isDeleted = hdfs.delete(path, isRecursive);
		  return isDeleted;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}
	
	public boolean existInHDFS(String hdfsPath) {
		try {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("mapred.jop.tracker", "hdfs://" + this.hadoopEnv.getMasterHost() + ":9001");
		conf.set("fs.defaultFS", "hdfs://" + this.hadoopEnv.getMasterHost() + ":" + this.hadoopEnv.getHdfsPort());
		  FileSystem hdfs = FileSystem.get(conf);
		  org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(hdfsPath);
		  boolean isExists = hdfs.exists(path);
		  return isExists;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}


	private boolean uploadFile(String localFile, String remoteFile, boolean deleteIfExists) {
		try {
			// TODO Auto-generated method stub
			String LOCAL_FILE = localFile;
			String REMOTE_FILE = remoteFile;

			if (!REMOTE_FILE.startsWith("/"))
				REMOTE_FILE = this.hadoopEnv.getAppRoot() + "/" + REMOTE_FILE;

			SshClient client = SshClient.setUpDefaultClient();
			client.start();

			ConnectFuture connect = client.connect(this.hadoopEnv.getMasterUser(), this.hadoopEnv.getMasterHost(),
					this.hadoopEnv.getMasterPort());
			boolean isSuccess = connect.await(5000L);

			if (isSuccess) {
				ClientSession session = connect.getSession();
				session.addPasswordIdentity(this.hadoopEnv.getMasterPassword());

				AuthFuture auth = session.auth();
				auth.await();
				if (!auth.isSuccess())
					System.out.println(
							"hadoop authenication failed, please check the availability of your hadoop environment.");

				SftpClient sftp = SftpClientFactory.instance().createSftpClient(session);
				
				Path src = Paths.get(LOCAL_FILE);

				if (deleteIfExists) {
					String dir = ".";
					String fileName = LOCAL_FILE;

					if (LOCAL_FILE.contains("/")) {
						dir = REMOTE_FILE.substring(0, REMOTE_FILE.lastIndexOf("/"));
						fileName = REMOTE_FILE.substring(REMOTE_FILE.lastIndexOf("/") + 1);
					}
					System.out.println("dir=" + dir);
					// sftp.mkdir(dir);
					System.out.println("fileName=" + fileName);

					boolean is_exist = false;
					for (DirEntry de : sftp.readDir(dir)) {
						//System.out.println(de.getFilename() + " " + de.getAttributes().getType());
						if (de.getFilename().equals(fileName)) {

							is_exist = true;
							System.out.println("exist file...");
						}
					}
					if (is_exist) {
						System.out.println("removing the jar file:"+REMOTE_FILE);
						sftp.remove(REMOTE_FILE);
					}

				}
				
				LogManager.getLogManager().reset();
				// if not exist directory, make one
				/*
				 * if(LOCAL_FILE.contains("/")) { String
				 * dir=LOCAL_FILE.substring(0,LOCAL_FILE.lastIndexOf("/")); sftp.mkdir(dir); }
				 */

				OutputStream os = sftp.write(REMOTE_FILE);
				Files.copy(src, os);
				os.close();

				//for (DirEntry de : sftp.readDir(".")) {
					//System.out.println(de.getFilename() + " " + de.getAttributes().getType());
				//}

				sftp.close();
				client.stop();

				return true;
			} else
				return false;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}

	private boolean downloadFile(String remoteFile, String downloadToFilePath) {
		try {
			// TODO Auto-generated method stub
			String DOWNLOAD_FILE = downloadToFilePath;
			String REMOTE_FILE = remoteFile;

			SshClient client = SshClient.setUpDefaultClient();
			client.start();

			ConnectFuture connect = client.connect(this.hadoopEnv.getMasterUser(), this.hadoopEnv.getMasterHost(),
					this.hadoopEnv.getMasterPort());
			boolean isSuccess = connect.await(5000L);

			if (isSuccess) {
				ClientSession session = connect.getSession();
				session.addPasswordIdentity(this.hadoopEnv.getMasterPassword());

				AuthFuture auth = session.auth();
				auth.await();
				if (!auth.isSuccess())
					System.out.println(
							"hadoop authenication failed, please check the availability of your hadoop environment.");

				SftpClient sftp = SftpClientFactory.instance().createSftpClient(session);

				InputStream is = sftp.read(REMOTE_FILE);
				Path dst = Paths.get(DOWNLOAD_FILE);
				Files.deleteIfExists(dst);
				Files.copy(is, dst);

				sftp.close();
				client.stop();

				return true;
			} else
				return false;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}

	public boolean uploadJar(HadoopJar hj) {

		boolean isu = uploadFile(hj.getLocalJarPath(), hj.getRemoteJarPath(), hj.isDeleteExistingFile());

		return isu;
	}

	public String submitTask(HadoopTask ht) {

		if (!ht.getJarRemotePath().startsWith("/"))
			ht.setJarRemotePath(this.hadoopEnv.getAppRoot() + "/" + ht.getJarRemotePath());

		if (ht.getArgs() != null && ht.getArgs().length > 0)
			for (int i = 0; i < ht.getArgs().length; i++)
				if (ht.getArgs()[i].startsWith("/"))
					ht.getArgs()[i] = "hdfs://" + this.hadoopEnv.getMasterHost() + ":9000" + ht.getArgs()[i];

		if (ht.getLogPath()==null||ht.getLogPath().isEmpty()) {
			ht.setLogPath("history");
		}
		
		String result = executeTask(ht);

		return result;
	}

	public boolean isSuccess(HadoopTask ht) {
		
		try {
			
			System.out.println("check If Spark job successfully done...");
			
			//obtain all files
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.hadoopEnv.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.hadoopEnv.getMasterHost() + ":" + this.hadoopEnv.getHdfsPort());
			FileSystem fs = FileSystem.get(conf);
			
			
			//System.out.println("successFlagFolder = " +this.get);

			SparkLog sparkLog=new SparkLog(this.hadoopEnv);
			
			String currentTraceId="";
			Map<String,String> app_args=InjectVars.getVars(ht.getArgs());
			if (app_args.containsKey("traceId")) {
				currentTraceId=app_args.get("traceId");
			}
			
			if(!currentTraceId.equals("")) {
				 
				System.out.println("currentTraceId="+currentTraceId);
				
			if(fs.exists(new org.apache.hadoop.fs.Path(sparkLog.getDonePath() + "/spark-id-" + currentTraceId))) {
				
				org.apache.hadoop.fs.Path pt=new org.apache.hadoop.fs.Path(sparkLog.getDonePath() + "/spark-id-" + currentTraceId);
				 
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				 String appId="";
				try {
				 
				  appId=br.readLine().trim();
				   
				} finally {
				  // you should close out the BufferedReader
				  br.close();
				}
				appId=appId.replace("\n","").trim();
				
				this.setAppId(appId);
				
				System.out.println(appId);
				
				if(fs.exists(new org.apache.hadoop.fs.Path(sparkLog.getDonePath() + "/"+appId))) {
					
					return true;
				}
				
				return false;
			}
			}
			
			
		

			return false;

		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}

	public boolean downloadResultToLocal(HadoopTask ht) {

		try {
			// obtain all files from HDFS
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("mapred.jop.tracker", "hdfs://" + this.hadoopEnv.getMasterHost() + ":9001");
			conf.set("fs.defaultFS", "hdfs://" + this.hadoopEnv.getMasterHost() + ":" + this.hadoopEnv.getHdfsPort());
			FileSystem fs = FileSystem.get(conf);
			
			System.out.println("output hdfs folders's number: "+ht.getOutputHdfsFolders().length);
			
			for (int i = 0; i < ht.getOutputHdfsFolders().length; i++) {

				String dir = ht.getOutputHdfsFolders()[i];
				
				if(dir==null||dir.isEmpty())
					continue;

				if (dir.startsWith("/"))
					dir = "hdfs://" + this.hadoopEnv.getMasterHost() + ":" + this.hadoopEnv.getHdfsPort() + dir;
				
				System.out.println("downloading from "+dir);
				
				if(!fs.exists(new org.apache.hadoop.fs.Path(dir))) {
					return false;
				}
				
				RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
						.listFiles(new org.apache.hadoop.fs.Path(dir), true);

				while (fileStatusListIterator.hasNext()) {
					LocatedFileStatus fileStatus = fileStatusListIterator.next();

					System.out.println("path=" + dir + "/" + fileStatus.getPath().getName());
					// 云端HDFS上的文件
					String CLOUD_DESC = dir + "/" + fileStatus.getPath().getName();
					// down到本地的文件
					String LOCAL_SRC = ht.getOutputLocalRootFolder() + "/" + fileStatus.getPath().getParent().getName()
							+ "/" + fileStatus.getPath().getName();
					// 获取conf配置
					
					if(!new File(ht.getOutputLocalRootFolder() + "/" + fileStatus.getPath().getParent().getName()).exists())
						new File(ht.getOutputLocalRootFolder() + "/" + fileStatus.getPath().getParent().getName()).mkdirs();

					// 读出流
					FSDataInputStream HDFS_IN = fs.open(new org.apache.hadoop.fs.Path(CLOUD_DESC));
					// 写入流
					OutputStream OutToLOCAL = new FileOutputStream(LOCAL_SRC);
					// 将InputStrteam 中的内容通过IOUtils的copyBytes方法复制到OutToLOCAL中
					IOUtils.copyBytes(HDFS_IN, OutToLOCAL, 1024, true);

				}
			}
			return true;
		}

		catch (Exception ex) {
			ex.printStackTrace();
		}

		return false;
	}
	
	 
	
	private String appId;
	
	public static void main(String[] args) {
		//spark-submit --class org.apache.spark.examples.JavaWordCount 
		//--master spark://master:7077 --executor-memory 500m --total-executor-cores 3 /usr/spark_apps/SparkTest.jar hdfs://master:9000/data/cdh/examples/wordcount/input
		Logger.getRootLogger().setLevel(Level.OFF);
		
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost("192.168.159.132");
		he.setMasterPassword("Passw0rd"); 
		he.setMasterPort(22); 
		he.setMasterUser("hadoop");
		he.setHadoopBin("/usr/spark/bin/spark-submit");
		he.setAppRoot("/usr/spark_apps");
		he.setSparkUrl("spark://master:7077");
		
		SparkApp sapp=new SparkApp(he);
		
		HadoopTask  ht=new HadoopTask();
		ht.setArgs(new String[] {"hdfs://master:9000/data/cdh/examples/wordcount/input"});
		ht.setMainClass("org.apache.spark.examples.JavaWordCount");
		ht.setOtherArgs(new String[] {
				"--executor-memory","500m",
				"--total-executor-cores", "3"
				});
		ht.setJarRemotePath(he.getAppRoot() + "/"+ "SparkTest.jar");
		ht.setAsync(false);
		
		String result=sapp.executeTask(ht);
		
		//String allOutputResult=sapp.getAllOutputResult();
		
		System.out.println("result = "+result);
		
		System.out.println("app Id: "+sapp.getAppId());
		
		SparkLog sparkLog=new SparkLog(he);
		sparkLog.extractByHDFS(sapp.getAppId(),  "C:/Users/douglaschan/Desktop/spark/tmp");
		
		DecimalFormat df = new DecimalFormat("#.00");
		
		System.out.println("Time cost of Spark App: "+ df.format(sparkLog.getTimeAppCost() * 1.0 /1000/60) +" mins");
		
		
	}
	
	private String findAppId(String result) {
		String[] lines=result.split("\n");
		for(int i=0;i<lines.length;i++) {
			if(lines[i].toLowerCase().contains("Connected to Spark Cluster".toLowerCase())) {
				int index=lines[i].indexOf("app-");
				return lines[i].substring(index).trim();
			}
		}
		return "";
	}

	public String getAllOutputResult() {
		return allOutputResult;
	}

	public void setAllOutputResult(String allOutputResult) {
		this.allOutputResult = allOutputResult;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}
	
}
