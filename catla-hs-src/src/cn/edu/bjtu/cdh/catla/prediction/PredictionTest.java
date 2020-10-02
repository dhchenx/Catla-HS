package cn.edu.bjtu.cdh.catla.prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.internal.chartpart.Chart;

import cn.edu.bjtu.cdh.catla.visualization.CatlaChart;
import cn.edu.bjtu.cdh.catla.visualization.ChartFactory;

public class PredictionTest {

	
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
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		List<String[]> data=readMetricFile(new File("E:\\CatlaHS\\bobyqa-two\\history\\timecost_cac_count.csv"));
		
		String[] x_fields=new String[] {"Order"};
		String[] y_fields=new String[] {"totalTimeCost"};
		
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
		
		
		
		/*
		
		double[] l2_result=MathUtil.lineFitting2(toParseDoubleMatrix(x_data), toParseDoubleList(y_data[0]));
		
		double[] predicted_ys=new double[x_data[0].length];
		for(int i=0;i<x_data[0].length;i++) {
			double predicted_y=MathUtil.getLine2ValueByX(toParseDoubleMatrix(x_data), new double[] {l2_result[0],l2_result[1],l2_result[2]},i);
			predicted_ys[i]=predicted_y;
		}
		
		*/
		
		
		double[][] x=toParseDoubleMatrix(x_data);
		double[] y=toParseDoubleList(y_data[0]);
		
		double[] l_result=MathUtil.dxsFitting(x[0], y, 3);
		double[] predicted_ys=new double[x_data[0].length];
		for(int i=0;i<x_data[0].length;i++) {
			double predicted_y=MathUtil.getDxsValueByX(x[0][i], new double[] {l_result[0],l_result[1],l_result[2],l_result[3] });
			predicted_ys[i]=predicted_y;
		}
		
		String[][] plot_x=new String[x_data[0].length][x_data.length];
		for(int i=0;i<plot_x.length;i++) {
			for(int j=0;j<plot_x[i].length;j++) {
				plot_x[i][j]=x_data[j][i];
			}
		}
		
		String[][] plot_y=new String[x_data[0].length][2];
		for(int i=0;i<plot_y.length;i++) {
			plot_y[i][0]=y_data[0][i];
			plot_y[i][1]=predicted_ys[i]+"";
		}
		
		printTable("x",plot_x);
		printTable("y",plot_y);
		
		CatlaChart chart=ChartFactory.createChart("line","Prediction summary",x_fields[0],"time cost",plot_x,plot_y,new String[] {"real","predicted"});
		
		new SwingWrapper<Chart>(chart.getChart()).displayChart();
		
		//double[] l_result=MathUtil.lineFitting(toParseDoubleMatrix(x_data)[0], toParseDoubleList(y_data[0]));
		
		
		
	}

}
