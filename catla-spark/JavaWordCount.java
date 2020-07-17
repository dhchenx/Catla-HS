/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    
    //spark.sparkContext().conf().set("spark.app.id", "myid-123456");
    
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
