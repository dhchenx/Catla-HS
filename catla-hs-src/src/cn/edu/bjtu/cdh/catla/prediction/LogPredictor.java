package cn.edu.bjtu.cdh.catla.prediction;

import java.util.Map;

import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.internal.chartpart.Chart;

import cn.edu.bjtu.cdh.catla.visualization.CatlaChart;
import cn.edu.bjtu.cdh.catla.visualization.ChartFactory;

public class LogPredictor extends CommonPredictor implements Predictor {
	// raw data
	private Map<String, String> options;
	private String[][] x_data, y_data;
	// estimated parameters for the prediction model
	private double[] model_result;
	// real transformed data for fitting model
	private double[][] x;
	private double[] y;
	// estimated/predicted y based on input x
	private double[] predicted_ys;
	// input and output x,y fields
	private String[] x_fields, y_fields;

	public LogPredictor(Map<String, String> options, String[] xfields, String[] yfields, String[][] xdata,
			String[][] ydata) {
		this.options = options;
		this.x_data = xdata;
		this.y_data = ydata;
		this.x_fields = xfields;
		this.y_fields = yfields;
	}

	public void fit() {
		this.x = toParseDoubleMatrix(x_data);
		this.y = toParseDoubleList(y_data[0]);
		this.model_result = MathUtil.logFitting(this.x[0], this.y);

	}

	public void predict() {
		double a=this.model_result[0];
		double b=this.model_result[1];
		
		double[] predicted_ys = new double[x_data[0].length];
		for (int i = 0; i < x_data[0].length; i++) {
			double predicted_y = Math.log(a*this.x[0][i]+b);
			predicted_ys[i] = predicted_y;
		}
		this.predicted_ys = predicted_ys;
	}

	public void plot() {

		// plot data transformation
		String[][] plot_x = new String[x_data[0].length][x_data.length];
		for (int i = 0; i < plot_x.length; i++) {
			for (int j = 0; j < plot_x[i].length; j++) {
				plot_x[i][j] = x_data[j][i];
			}
		}

		String[][] plot_y = new String[x_data[0].length][2];
		for (int i = 0; i < plot_y.length; i++) {
			plot_y[i][0] = y_data[0][i];
			plot_y[i][1] = predicted_ys[i] + "";
		}

		// print plot dataset
		printTable("x", plot_x);
		printTable("y", plot_y);

		String plot_type = options.getOrDefault("-plot_type", "line");
		String plot_title = options.getOrDefault("-plot_title", "");

		// show graph
		CatlaChart chart = ChartFactory.createChart(plot_type, plot_title, x_fields[0], y_fields[0], plot_x, plot_y,
				new String[] { "real", "predicted" });
		new SwingWrapper<Chart>(chart.getChart()).displayChart();
	}

	public void plotError() {
		String[][] plot_x = new String[y_data[0].length][1];
		for (int i = 0; i < y_data[0].length; i++) {
			plot_x[i][0] = y_data[0][i];
		}

		String[][] plot_y = new String[x_data[0].length][1];
		for (int i = 0; i < plot_y.length; i++) {

			plot_y[i][0] = predicted_ys[i] + "";
		}

		// print plot dataset
		printTable("x", plot_x);
		printTable("y", plot_y);

		// show graph
		CatlaChart chart = ChartFactory.createChart("scatter", "difference between real and predicted values", "real",
				"predicted", plot_x, plot_y, new String[] { "difference" });
		new SwingWrapper<Chart>(chart.getChart()).displayChart();
	}

}
