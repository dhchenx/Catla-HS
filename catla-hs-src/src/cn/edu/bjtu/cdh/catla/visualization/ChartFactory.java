package cn.edu.bjtu.cdh.catla.visualization;

public class ChartFactory {
	public static CatlaChart createChart(String type,String title, String xTitle, String yTitle, String[][] X, String[][] Y, String[] seriesName) {
		if (type.equals("line")) {
			return new XYsChart(title,xTitle,yTitle,X,Y,seriesName);
		}else if(type.equals("bar")) {
			return new BarChart(title,xTitle,yTitle,X,Y,seriesName);
		}else if(type.equals("simpleline")) {
			return new SimpleXYChart(title,xTitle,yTitle,X,Y,seriesName);
		}else if(type.equals("scatter")) {
			return new ScatterChart(title,xTitle,yTitle,X,Y,seriesName);
		}
		else
		{
			return null;
		}
	}
}
