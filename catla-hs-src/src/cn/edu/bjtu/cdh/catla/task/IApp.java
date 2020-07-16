package cn.edu.bjtu.cdh.catla.task;

public interface IApp {
	
	public String executeTask(HadoopTask ht) ;
	public String submitTask(HadoopTask ht);
	public boolean uploadJar(HadoopJar hj);
	public boolean isSuccess(HadoopTask ht) ;
	public boolean downloadResultToLocal(HadoopTask ht);
	public boolean deleteInHDFS(String hdfsPath,boolean isRecursive);
	public boolean existInHDFS(String hdfsPath);
	
}
