package cn.edu.bjtu.cdh.catla.examples;

import cn.edu.bjtu.cdh.catla.CatlaRunner;

public class SparkTaskExample {

	public static void main1(String[] args) {
		// TODO Auto-generated method stub
		// Submit a simple MapReduce Job based on task template
		args=new String[] {
				"-tool","task",
				"-dir","C:\\Users\\douglaschan\\Desktop\\spark\\task_wordcount"
		};
		
		CatlaRunner.main(args);
	}
	
	public static void main2(String[] args) {
		// Execute a series of complicated MapReduce job operations based on project template
		args=new String[] {
				"-tool","project",
				"-dir","C:\\Users\\douglaschan\\Desktop\\spark\\project_wordcount",
				"-task","pipeline",
				"-download","true",
				"-sequence","true"
		};
		CatlaRunner.main(args);	
	}
	
	public static void main(String[] args) {

		//tuning using Exhaustive Search
		try {
			
		args = new String[] { 
					"-tool","tuning",
					"-dir", "C:\\Users\\douglaschan\\Desktop\\spark\\tuning_wordcount_spark",
					"-clean", "true", 
					"-group", "wordcount", 
					"-upload","false",
					"-uploadjar","true",
				};
			
			CatlaRunner.main(args);
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}

	}
	
}



