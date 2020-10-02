package cn.edu.bjtu.cdh.catla.prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.edu.bjtu.cdh.catla.task.HadoopTask;
import cn.edu.bjtu.cdh.catla.tuning.HadoopTuning;

public class PredictorRunner {

	private static String findTimeCostFile(String historyFolder) {
		File folder=new File(historyFolder);
		File[] files=folder.listFiles();
		for(int i=0;i<files.length;i++) {
			if(files[i].isFile() && files[i].getName().startsWith("timecost")) {
				return files[i].getAbsolutePath();
			}
		}
		return null;
	}
	
	private static List<String[]> readMetricFile(File fin) throws IOException {
	    // Construct BufferedReader from FileReader
	    BufferedReader br = new BufferedReader(new FileReader(fin));
	 
	    List<String[]> ls=new ArrayList<String[]>();
	    
	    String line = null;
	    while ((line = br.readLine()) != null) {
	       
	        if(line.trim().equals(""))
	        	continue;
	        
	        System.out.println(line);
	        
	        ls.add(line.split("\t"));
	    }
	    
	    br.close();
	    
	    return ls;
	}
	
	private static int findIdByName(String[] list,String name) {
		for(int i=0;i<list.length;i++) {
			if(list[i].toLowerCase().equals(name.toLowerCase())) {
				return i;
			}
		}
		return -1;
	}
	
	public static void printTable(String tag,Object[][] data) {
		System.out.println("-----"+tag+"-----");
		for(int i=0;i<data.length;i++) {
			String line="";
			for(int j=0;j<data[i].length;j++) {
				line+=(String)data[i][j]+"\t";
			}
			line=line.trim();
			System.out.println(line);
		}
		System.out.println();
		
	}
	
	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
		try {
		
		if(args.length==0) {
			args=new String[] {
					"-dir","E:\\CatlaHS\\bobyqa-two\\history",
					"-predictor","lineXs",
					"-x","mapreduce.map.sort.spill.percent,mapred.reduce.tasks",
					"-y","totalTimeCost",
					"-plot_type","line",
					"-plot_title","Tuning summary",
					"-n","3"
			};
		}
		
		URL rootUrl = HadoopTask.class.getProtectionDomain().getCodeSource().getLocation();
		String jarFolder = URLDecoder.decode(rootUrl.getPath(), "utf-8");
	
		if (jarFolder.endsWith(".jar")) {
			jarFolder = jarFolder.substring(0, jarFolder.lastIndexOf("/"));
			if (jarFolder.contains(":"))
				jarFolder = jarFolder.substring(1);
	
		} else {
			jarFolder = "";
		}
	
		Map<String, String> options = HadoopTuning.getOptionMap(args);
	
		String dirFolder = options.get("-dir");
	
		if (!jarFolder.isEmpty()) {
			if (!options.get("-dir").contains(":") && !options.get("-dir").startsWith("/")) {
				dirFolder = jarFolder + "/" + options.get("-dir");
			}
		}
		
		if (!new File(dirFolder).exists()) {
			System.out.println("Folder: " + dirFolder + " does not exist.");
			return;
		}
		
		String timecost_path=null;
		
		if(!new File(dirFolder+"/_hproj.txt").exists()) {
			System.out.println("This call is considered as history folder");
			timecost_path=findTimeCostFile(dirFolder);
		}else {
			System.out.println("This call is considered as project folder");
			timecost_path=findTimeCostFile(dirFolder+"/" + "history");
				
		}
		
		// analysis
		
		System.out.println("Used timecost path: "+timecost_path);
		
		List<String[]> data=readMetricFile(new File(timecost_path));
		
		String[] x_fields=options.get("-x").split(",");
		String[] y_fields=options.get("-y").split(",");
		
		String[][] x_data=new String[x_fields.length][data.size()-1];
		String[][] y_data=new String[y_fields.length][data.size()-1];
		
		int[] x_ids=new int[x_fields.length];
		int[] y_ids=new int[y_fields.length];
		
		for(int i=0;i<x_fields.length;i++) {
			x_ids[i]=findIdByName(data.get(0),x_fields[i]);
			 
		}
		
		for(int i=0;i<y_fields.length;i++) {
		 
			y_ids[i]=findIdByName(data.get(0),y_fields[i]);
		}
		
		for(int i=0;i<x_ids.length;i++) {
			for(int j=1;j<data.size();j++) {
				x_data[i][j-1]=data.get(j)[x_ids[i]];
			}
			
		}
		
		for(int i=0;i<y_ids.length;i++) {
			for(int j=1;j<data.size();j++) {
				y_data[i][j-1]=data.get(j)[y_ids[i]];
			}
			
		}
		
		printTable("X",x_data);
		printTable("Y",y_data);
		
		
		Predictor pred=PredictorFactory.createInstance(options, options.get("-predictor"),x_fields,y_fields, x_data, y_data);
		
		pred.fit();
		
		pred.predict();
		
		pred.plot();
		
		pred.plotError();

		}catch(Exception ex) {
			ex.printStackTrace();
		}
		
		
	}

}
