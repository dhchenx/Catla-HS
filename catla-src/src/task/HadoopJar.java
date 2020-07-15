package cn.edu.bjtu.cdh.catla.task;

import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;

public class HadoopJar {
	
	private String localJarPath;
	private String remoteJarPath;
	public String getLocalJarPath() {
		return localJarPath;
	}
	public void setLocalJarPath(String localJarPath) {
		this.localJarPath = localJarPath;
	}
	public String getRemoteJarPath() {
		return remoteJarPath;
	}
	public void setRemoteJarPath(String remoteJarPath) {
		this.remoteJarPath = remoteJarPath;
	}
	
	private boolean deleteExistingFile;
	public boolean isDeleteExistingFile() {
		return deleteExistingFile;
	}
	
	public void setDeleteExistingFile(boolean deleteExistingFile) {
		this.deleteExistingFile = deleteExistingFile;
	}
	
	
	
	public void saveToText(String path) {
		String line="";
		line+="Task"+" upload\r\n";
		line+="LocalJarPath"+" "+this.getLocalJarPath()+"\r\n";
		line+="RemoteJarPath"+" "+this.getRemoteJarPath()+"\r\n";
		line+="DeleteExistingFile"+" "+this.isDeleteExistingFile();
		CatlaFileUtils.writeFile(path, line);
	}
	
}
