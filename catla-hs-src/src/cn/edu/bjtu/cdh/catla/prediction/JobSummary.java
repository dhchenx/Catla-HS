package cn.edu.bjtu.cdh.catla.prediction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.internal.chartpart.Chart;

import cn.edu.bjtu.cdh.catla.visualization.BarChart;
import cn.edu.bjtu.cdh.catla.visualization.CatlaChart;
import cn.edu.bjtu.cdh.catla.visualization.ChartFactory;
import cn.edu.bjtu.cdh.catla.visualization.XYsChart;

public class JobSummary {

	public static void main(String[] args) {
		String data_path="E:\\CatlaHS\\bobyqa-two\\history\\timecost_cac_count.csv";
		JobSummary ts=new JobSummary(data_path);
		ts.load("Order",new String[] {"totalTimeCost" , "jobTimeCost"},"bar");
	}
	
 
	private String data_path="";
	
	public JobSummary(String data_path) {
 
		this.data_path=data_path;
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

	private List<String[]> readMetricFile(File fin) throws IOException {
		// Construct BufferedReader from FileReader
		BufferedReader br = new BufferedReader(new FileReader(fin));

		List<String[]> ls = new ArrayList<String[]>();

		String line = null;
		while ((line = br.readLine()) != null) {

			if (line.trim().equals(""))
				continue;

			System.out.println(line);

			ls.add(line.split("\t"));
		}

		br.close();
		return ls;
	}

	private int findIdByName(String[] list, String name) {
		for (int i = 0; i < list.length; i++) {
			if (list[i].toLowerCase().equals(name.toLowerCase())) {
				return i;
			}
		}
		return -1;
	}

	public void load() {
		this.load("Order");
	}
	
	public void load(String x) {
		this.load(x,new String[] { "totalTimeCost" , "jobTimeCost","avgMapTimeCost","avgReduceTimeCost","avgShuffleTimeCost","avgSortTimeCost","setupTimeCost"},"line");
	}
	
	public void load(String x,String[] ys,String plot_type) {

		try {

			List<String[]> data = readMetricFile(
					new File(data_path));

			String[] x_fields = new String[] { x};
			String[] y_fields = ys;

			String[][] x_data = new String[data.size() - 1][x_fields.length];
			String[][] y_data = new String[data.size() - 1][y_fields.length];

			int[] x_ids = new int[x_fields.length];
			int[] y_ids = new int[y_fields.length];

			for (int i = 0; i < x_fields.length; i++) {
				x_ids[i] = findIdByName(data.get(0), x_fields[i]);
				
			}
			
			for (int i = 0; i < y_fields.length; i++) {
				y_ids[i] = findIdByName(data.get(0), y_fields[i]);
				
			}
			

			for (int i = 1; i < data.size(); i++) {
				for (int j = 0; j < x_ids.length; j++) {
					x_data[i - 1][j] = data.get(i)[x_ids[j]];
				}
				for (int j = 0; j < y_ids.length; j++) {
					y_data[i - 1][j] = data.get(i)[y_ids[j]];
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
}
