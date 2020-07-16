# Catla-HS for Spark

The Catla-HS project supports executing, monitoring and tuning Spark MapReduce jobs. 

Here illustrates the basic idea of supporting Spark clusters. 

## Basic functions
1. To run MapReduce jobs written in Java and then obtain outputs and spark logs. 
2. To run a series of Spark jobs in an organized project to achieve specific purposes in complicated application scenes. 
3. To obtain optimal parameter settings for Spark jobs using direct search methods.
4. To obtain optimal parameter settings for Spark jobs using derivative-free optimization methods, which is suitable for cases with large search space of parameter. 

## Environment set-up
1. Spark should be properly set up on existing Hadoop clusters. 
2. User information of master machine should be known in advance. 
3. Catla-HS and the Spark cluster are in the same LAN.

## Usage and examples
There are three mode of tasks in CatlaHS designed to automate configuration of Spark jobs when running on clusters:
1) Task
2) Project
3) Optimize

### submit a Spark task
```java
        args=new String[] {
				"-tool","task",
				"-dir","task_wordcount"
		};
		
		CatlaRunner.main(args);
```

### submit a Spark project
```java
        args=new String[] {
				"-tool","project",
				"-dir","project_wordcount",
				"-task","pipeline",
				"-download","true",
				"-sequence","true"
		};
		CatlaRunner.main(args);	
```
### submit a Spark tuning project
```java
	  args = new String[] { 
					"-tool","tuning",
					"-dir", "tuning_wordcount",
					"-clean", "true", 
					"-group", "wordcount", 
					"-upload","false",
					"-uploadjar","true",
				};
			
			CatlaRunner.main(args);
```
Examples of defining settings of parameters:
```bat
executor-memory={512m,700m}:string
total-executor-cores=[1,2],1:int
```
Examples of defining current parameters used during the tuning process
```bat
total-executor-cores
executor-memory
```

## Examples in real life
1. SparkTest: Java project that contains source codes of Spark jobs we used in this test. 
2. task_wordcount: a Catla-HS task that defined configuration of executing Spark jobs. 
3. SparkTaskExample: an Java example that performs execution of Spark jobs uisng Catla-HS. 

## Design of a Spark job in Catla-HS
JavaWordCount as an example:
```java
package org.apache.spark.examples;

import scala.Tuple2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }
    
	Map<String,String> app_args=InjectVars.getVars(args);
	String[] otherArgs=InjectVars.getArgs(args);
	
	String app_name="SparkWordCount";
	if(app_args.containsKey("traceId")) {
		app_name+="["+app_args.get("traceId")+"]";
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
      
      .appName(app_name)
      .getOrCreate();
    
    String appId=spark.sparkContext().applicationId();
    
    System.out.println("*spark.app.Id="+appId);
    
    // write app.id files
    Configuration conf = new Configuration();
    conf.set("mapred.jop.tracker", "hdfs://" + master_ip + ":9001");
	conf.set("fs.defaultFS", "hdfs://" +master_ip + ":9000");

	FileSystem fs = FileSystem.get(conf);
	Path filenamePath = new Path("hdfs://" +master_ip + ":9000/"+history_url+"/spark-id-"+app_args.get("traceId"));  
	try {
	    if (fs.exists(filenamePath)) {
	        fs.delete(filenamePath, true);
	    }

	    FSDataOutputStream fin = fs.create(filenamePath);
	    fin.write(new String(appId).getBytes());
	    fin.close();
	}catch(Exception ex) {
		ex.printStackTrace();
	}

    JavaRDD<String> lines = spark.read().textFile(otherArgs[0]).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
  }
}


```

