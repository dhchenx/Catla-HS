package cn.edu.bjtu.cdh.catla.examples;

import cn.edu.bjtu.cdh.catla.CatlaRunner;

public class SparkTaskExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Submit a simple MapReduce Job based on task template
		args=new String[] {
				"-tool","task",
				"-dir","C:\\Users\\douglaschan\\Desktop\\spark\\task_wordcount"
		};
		
		CatlaRunner.main(args);
	}

}
