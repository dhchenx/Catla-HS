package cn.edu.bjtu.cdh.catla.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.internal.chartpart.Chart;

import cn.edu.bjtu.cdh.catla.visualization.CatlaChart;
import cn.edu.bjtu.cdh.catla.visualization.ChartFactory;

public class TuningSummary {
	
	private String[] projects;
	
	public TuningSummary(String[] projects) {
		this.projects=projects;
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
	
	public String[][] read() {
		// TODO Auto-generated method stub
		String[] metrics=new String[] {"Application","TotalTuningTime","AvgTuningTime","AvgJobTime","DifferenceOfTuningTime",
				"NumberOfTunings","MaxTimeStamp","MinTimeStamp","MaxJobTime","MinJobTime","MaxJobTimeId","MinJobTimeId","MaxTuningTime",
				"MinTuningTime","MaxTuningId","MinTuningId"};
		String[][] data=new String[this.projects.length+1][metrics.length] ;
		data[0]=metrics;
		
		for(int i=0;i<this.projects.length;i++) {
			TuningMetrics tm=new TuningMetrics(projects[i]);
			tm.extract("wordcount");
			System.out.println(tm.getProjectName()+"\t"+tm.getMinTuningTime()+"\t"+tm.getMinJobTime());
			
			String[] lines=new String[] {
					tm.getProjectName(),
					tm.getTotalTuningTime()+"",
					tm.getAvgTuningTime()+"",
					tm.getAvgJobTime()+"",
					tm.getDifferenceOfTuningTime()+"",
					tm.getNumberOfTunings()+"",
					tm.getMaxTimeStamp()+"",
					tm.getMinTimeStamp()+"",
					tm.getMaxJobTime()+"",
					tm.getMinJobTime()+"",
					tm.getMaxJobTimeId()+"",
					tm.getMinJobTimeId()+"",
					tm.getMaxTuningTime()+"",
					tm.getMinTuningTime()+"",
					tm.getMaxTuningId()+"",
					tm.getMinTuningId()+""
			};
			
			data[i+1]=lines;
			
		}
		return data;
		
		
	
		
	}
	

	private int findIdByName(String[] list, String name) {
		for (int i = 0; i < list.length; i++) {
			if (list[i].toLowerCase().equals(name.toLowerCase())) {
				return i;
			}
		}
		return -1;
	}
	
	public void load(String x,String[] ys,String plot_type) {

		try {

			String[][] data=read();

			String[] x_fields = new String[] { x};
			String[] y_fields = ys;

			String[][] x_data = new String[data.length - 1][x_fields.length];
			String[][] y_data = new String[data.length - 1][y_fields.length];

			int[] x_ids = new int[x_fields.length];
			int[] y_ids = new int[y_fields.length];

			for (int i = 0; i < x_fields.length; i++) {
				x_ids[i] = findIdByName(data[0], x_fields[i]);
				
			}
			
			for (int i = 0; i < y_fields.length; i++) {
				y_ids[i] = findIdByName(data[0], y_fields[i]);
				
			}
			

			for (int i = 1; i < data.length; i++) {
				for (int j = 0; j < x_ids.length; j++) {
					x_data[i - 1][j] = data[i][x_ids[j]];
				}
				for (int j = 0; j < y_ids.length; j++) {
					y_data[i - 1][j] = data[i][y_ids[j]];
				}
			}
			
			
			CatlaChart chart=ChartFactory.createChart(plot_type,"Tuning summary",x_fields[0],"time cost",x_data,y_data,y_fields);
		
			new SwingWrapper<Chart>(chart.getChart()).displayChart();
					
			printTable("X",x_data);
			printTable("Y",y_data);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String rootPath="E:\\CatlaHS\\";
		String[] projects=new String[] {rootPath+"tuning_reducejoin",rootPath+"tuning_terasort",rootPath+"tuning_wordcount"};
		TuningSummary ts=new TuningSummary(projects);
		ts.load("Application", new String[] {"AvgTuningTime","AvgJobTime"}, "bar");
	}

}
