package cn.edu.bjtu.cdh.catla.spark;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public class CatlaSparkSession {
	public static SparkSession creatInstance(String appName,String[] args) {
		
		try {
		Map<String,String> app_args=InjectVars.getVars(args);
		String[] otherArgs=InjectVars.getArgs(args);

		if(app_args.containsKey("traceId")) {
			appName+="["+app_args.get("traceId")+"]";
		}
		
		String master_ip="master";
		if(app_args.containsKey("masterIP")) {
			master_ip =app_args.get("masterIP");
		}
		
		String history_url="spark-job-history";
		if(app_args.containsKey("historyUrl")) {
			history_url =app_args.get("historyUrl");
		}

	    SparkSession spark = SparkSession
	      .builder()
	      
	      .appName(appName)
	      .getOrCreate();
	    
	    //spark.sparkContext().conf().set("spark.app.id", "myid-123456");
	    
	    String appId=spark.sparkContext().applicationId();
	    
	    System.out.println("*spark.app.Id="+appId);
	    
	    // write app.id files
	    Configuration conf = new Configuration();
	    conf.set("mapred.jop.tracker", "hdfs://" + master_ip + ":9001");
		conf.set("fs.defaultFS", "hdfs://" +master_ip + ":9000");

		FileSystem fs = FileSystem.get(conf);
		Path filenamePath = new Path("hdfs://" +master_ip + ":9000/"+history_url+"/spark-id-"+app_args.get("traceId"));  
		
		    if (fs.exists(filenamePath)) {
		        fs.delete(filenamePath, true);
		    }

		    FSDataOutputStream fin = fs.create(filenamePath);
		    fin.write(new String(appId).getBytes());
		    fin.close();
		    
		    return spark;
		}catch(Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}
}
