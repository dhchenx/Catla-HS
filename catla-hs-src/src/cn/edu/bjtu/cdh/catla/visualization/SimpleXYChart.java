package cn.edu.bjtu.cdh.catla.visualization;

import java.util.ArrayList;
import java.util.List;

import org.knowm.xchart.*;
import org.knowm.xchart.internal.chartpart.Chart;
import org.knowm.xchart.style.Styler.ChartTheme;
import org.knowm.xchart.style.markers.SeriesMarkers;

public class SimpleXYChart implements CatlaChart {

	public static void main(String[] args) {
		/*
		MultipleXYChart exampleChart = new MultipleXYChart();
		Chart chart = exampleChart.getChart();
		new SwingWrapper<Chart>(chart).displayChart();
		*/
	}
	
	private String title;
	private String x_title;
	private String y_title;
	private String[][] x_data;
	private String[][] y_data;
	private String[] seriesNames;
	
	public SimpleXYChart(String title,String xTitle,String yTitle,String[][] X,String[][] Y,String[] seriesNames) {
		this.title=title;
		this.x_title=xTitle;
		this.y_title=yTitle;
		this.x_data=X;
		this.y_data=Y;
		this.seriesNames=seriesNames;
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
	 
	public Chart getChart() {

		// Create Chart
		XYChart chart = new XYChartBuilder().width(800).height(600).theme(ChartTheme.Matlab).title(this.title)
				.xAxisTitle(this.x_title).yAxisTitle(this.y_title).build();

		// Customize Chart
		chart.getStyler().setPlotGridLinesVisible(false);
		chart.getStyler().setXAxisTickMarkSpacingHint(100);

		// Series
		List<List<Double>> x_series=array2List(x_data);
		List<List<Double>> y_series=array2List(y_data);
		
		// System.out.println(x_series.size());
		

		XYSeries series = chart.addSeries(seriesNames[0],x_series.get(0),y_series.get(0));
		series.setMarker(SeriesMarkers.CIRCLE);
		
			

		return chart;
	}


}