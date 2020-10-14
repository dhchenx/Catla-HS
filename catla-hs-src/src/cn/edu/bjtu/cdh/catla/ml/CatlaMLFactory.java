package cn.edu.bjtu.cdh.catla.ml;

public class CatlaMLFactory {
	
	public static String JavaMachineLearning="java-ml";
	public static String Mahout="mahout";
	
	public static CatlaML createInstance(String lib,String project_folder,String algorithm) {
		if(lib.equals(JavaMachineLearning)) {
			return new CatlaJavaML(project_folder,algorithm);
		}else if (lib.equals(Mahout)){
			return new CatlaMahout();
		}else {
			return null;
		}
	}
}

