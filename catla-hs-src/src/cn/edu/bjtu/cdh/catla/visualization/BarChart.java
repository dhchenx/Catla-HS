package cn.edu.bjtu.cdh.catla.visualization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler.ChartTheme;
import org.knowm.xchart.style.Styler.LegendPosition;


public class BarChart implements CatlaChart {

	public static void main(String[] args) {
		/*
		 * BasicBarChart exampleChart = new BasicBarChart(); Chart chart =
		 * exampleChart.getChart(); new SwingWrapper<Chart>(chart).displayChart();
		 */
	}

	private String title;
	private String x_title;
	private String y_title;
	private String[][] x_data;
	private String[][] y_data;
	private String[] seriesNames;

	public BarChart(String title, String xTitle, String yTitle, String[][] X, String[][] Y, String[] seriesNames) {
		this.title = title;
		this.x_title = xTitle;
		this.y_title = yTitle;
		this.x_data = X;
		this.y_data = Y;
		this.seriesNames = seriesNames;
	}
	
	private List<List<Double>> array2List(String[][] data){
		List<List<Double>> list=new ArrayList<List<Double>>();
		for(int i=0;i<data[0].length;i++) {
			List<Double> dlist=new ArrayList<Double>();
			for(int j=0;j<data.length;j++) {
				dlist.add(Double.parseDouble(data[j][i]));
			}
			list.add(dlist);
		}
		return list;
	}
	
	private List<List<String>> array2ListWithString(String[][] data){
		List<List<String>> list=new ArrayList<List<String>>();
		for(int i=0;i<data[0].length;i++) {
			List<String> dlist=new ArrayList<String>();
			for(int j=0;j<data.length;j++) {
				dlist.add(data[j][i]);
			}
			list.add(dlist);
		}
		return list;
	}

	public Chart getChart() {

		// Create Chart
		CategoryChart chart = new CategoryChartBuilder().theme(ChartTheme.Matlab).width(800).height(600)
				.title(this.title).xAxisTitle(this.x_title).yAxisTitle(this.y_title).build();

		// Customize Chart
		chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
		chart.getStyler().setHasAnnotations(false);

		// Series
		// Series
				List<List<String>> x_series=array2ListWithString(x_data);
				List<List<Double>> y_series=array2List(y_data);
			
					for(int j=0;j<y_series.size();j++) {
						
						chart.addSeries(seriesNames[j], x_series.get(0),
								y_series.get(j));
						
					}

		return chart;
	}
}