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
	
	public static void main(String[] args) {
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
}



