package cn.edu.bjtu.cdh.catla.task;

public class AppFactory {
	public static IApp createApp(HadoopEnv env,String name) {
		if(name.equals("Spark")) {
			return new SparkApp(env);
		}else
		if(name.equals("Hadoop")) {
			return new HadoopApp(env);
		}else
			return new HadoopApp(env);
	}
}
