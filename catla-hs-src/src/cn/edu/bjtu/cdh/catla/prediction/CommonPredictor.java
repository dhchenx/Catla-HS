package cn.edu.bjtu.cdh.catla.prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonPredictor {

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
	
	public static double[] toParseDoubleList(String[] list){
		double[] dlist=new double[list.length];
		for(int i=0;i<list.length;i++) {
			dlist[i]=Double.parseDouble(list[i]);
		}
		return dlist;
	}
	
	public static double[][] toParseDoubleMatrix(String[][] m){
		double[][] mm=new double[m.length][m[0].length];
		for(int i=0;i<m.length;i++) {
			for(int j=0;j<m[i].length;j++)
				mm[i][j]=Double.parseDouble(m[i][j]);
		}
		return mm;
	}
	
	
	public static String[] getOneColumn(String[][] data, int index) {
		String[] dlist=new String[data.length];
		for(int i=0;i<dlist.length;i++) {
			dlist[i]=data[i][index];
		}
		return dlist;
	}
	
}
