package cn.edu.bjtu.cdh.catla.examples;

import cn.edu.bjtu.cdh.catla.CatlaRunner;

public class CatlaExamples {
	//Typical use of Catla
	public static void main(String[] args) {

	// TODO Auto-generated method stub
	//  testSubmitTask();
	//	testOptmizer();
	//	testSubmitProject();
	//	testTuningUsingExhaustiveSearch();
		testExportHadoopLog();
	}
	
	public static void testTuningUsingExhaustiveSearch() {

		//tuning using Exhaustive Search
		try {
			
		String[] args = new String[] { 
					"-tool","tuning",
					"-dir", "C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\Catla\\tuning_wordcount",
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
	
	public static void testTuningByOptmizer() {

		//tuning using the BOBYQA DFO-based optimizer
		try {
			
		String[]	args = new String[] { 
					"-tool","optimizer",
					"-dir", "C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\Catla\\tuning_wordcount",
					"-clean", "true", 
					"-group", "wordcount", 
					"-upload","true",
					"-uploadjar","true",
					"-maxinter","1000",
					"-optimizer","BOBYQA",
					"-BOBYQA-initTRR","20",
					"-BOBYQA-stopTRR","1.0e-4"
				};
			
			CatlaRunner.main(args);
			
		}catch(Exception ex) {
			ex.printStackTrace();
		}

	}
	
	public static void testSubmitTask() {
		// Submit a simple MapReduce Job based on task template
		String[] args=new String[] {
				"-tool","task",
				"-dir","C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\Catla\\task_wordcount"
		};
		
		CatlaRunner.main(args);
		
	}
	
	public static void testSubmitProject() {
		// Execute a series of complicated MapReduce job operations based on project template
		String[] args=new String[] {
				"-tool","project",
				"-dir","C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\Catla\\project_wordcount",
				"-task","pipeline",
				"-download","true",
				"-sequence","true"
		};
		
		CatlaRunner.main(args);
		
	}
	
	public static void testExportHadoopLog() {
		// Export a series of logs and aggregate their information of time cost in each MapReduce phrase into a table form
		String[] args=new String[] {
				"-tool","log",
				"-dir","C:\\Users\\douglaschan\\Desktop\\Hadoop任务管理工具\\Catla\\tuning_wordcount"
		};
		
		CatlaRunner.main(args);
		
	}

}
