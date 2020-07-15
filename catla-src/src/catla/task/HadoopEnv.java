package cn.edu.bjtu.cdh.catla.task;

import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;

public class HadoopEnv {
	private String masterHost;
	private int masterPort=22;
	
	private int hdfsPort=9000;
	
	private String sparkUrl;
	
	private String hadoopBin="/usr/hadoop/bin/hadoop";
	
	public int getHdfsPort() {
		return hdfsPort;
	}
	public void setHdfsPort(int hdfsPort) {
		this.hdfsPort = hdfsPort;
	}
	public String getHadoopBin() {
		return hadoopBin;
	}
	public void setHadoopBin(String hadoopBin) {
		this.hadoopBin = hadoopBin;
	}
	public int getMasterPort() {
		return masterPort;
	}
	public void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}
	public String getMasterHost() {
		return masterHost;
	}
	public void setMasterHost(String masterHost) {
		this.masterHost = masterHost;
	}
	public String getMasterUser() {
		return masterUser;
	}
	public void setMasterUser(String masterUser) {
		this.masterUser = masterUser;
	}
	public String getMasterPassword() {
		return masterPassword;
	}
	public void setMasterPassword(String masterPassword) {
		this.masterPassword = masterPassword;
	}
	private String masterUser;
	private String masterPassword;
	
	public String getAppRoot() {
		return appRoot;
	}
	public void setAppRoot(String appRoot) {
		this.appRoot = appRoot;
	}
	private String appRoot;
	
	public void saveToFile(String path) {
		String line="";
		line+="Task"+" "+"init"+"\r\n";
		line+="MasterHost"+" "+this.masterHost+"\r\n";
		line+="MasterPassword"+" "+this.masterPassword+"\r\n";
		line+="MasterPort"+" "+this.masterPort+"\r\n";
		line+="MasterUser"+" "+this.masterUser+"\r\n";
		line+="HadoopBin"+" "+this.hadoopBin+"\r\n";
		line+="AppRoot"+" "+this.appRoot;
		CatlaFileUtils.writeFile(path, line);
	}
	public String getSparkUrl() {
		return sparkUrl;
	}
	public void setSparkUrl(String sparkUrl) {
		this.sparkUrl = sparkUrl;
	}
	
	
}
