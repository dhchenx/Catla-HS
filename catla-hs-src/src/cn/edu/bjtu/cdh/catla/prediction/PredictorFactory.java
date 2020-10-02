package cn.edu.bjtu.cdh.catla.prediction;

import java.util.Map;

public class PredictorFactory {
	public static Predictor createInstance(Map<String,String>options,String type,String[] xfields,String[] yfields, String[][] x_data,String[][] y_data) {
		if(type.equals("poly")) {
			return new PolyPredictor(options,xfields,yfields,x_data,y_data);
		}else if (type.equals("line")) {
			return new LinePredictor(options,xfields,yfields,x_data,y_data);
		}else if (type.equals("lineXs")) {
			return new LineXsPredictor(options,xfields,yfields,x_data,y_data);
		}else if (type.equals("exp")) {
			return new ExpPredictor(options,xfields,yfields,x_data,y_data);
		}else if (type.equals("log")) {
			return new LogPredictor(options,xfields,yfields,x_data,y_data);
		}
		return null;
	}
}
