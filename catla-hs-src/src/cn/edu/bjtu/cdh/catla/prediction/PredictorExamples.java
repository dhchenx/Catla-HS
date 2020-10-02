package cn.edu.bjtu.cdh.catla.prediction;

public class PredictorExamples {

	public static void testPoly()  {
		String[] args=new String[] {
				"-dir","E:\\CatlaHS\\bobyqa-two\\history",
				"-predictor","poly",
				"-x","Order",
				"-y","totalTimeCost",
				"-plot_type","line",
				"-plot_title","Tuning summary",
				"-n","3"
		};
		PredictorRunner.main(args);
	}
	
	public static void testLine()  {
		String[] args=new String[] {
				"-dir","E:\\CatlaHS\\bobyqa-two\\history",
				"-predictor","line",
				"-x","Order",
				"-y","totalTimeCost",
				"-plot_type","line",
				"-plot_title","Tuning summary",
				"-n","3"
		};
		PredictorRunner.main(args);
	}
	
	public static void testLine2()  {
		String[] args=new String[] {
				"-dir","E:\\CatlaHS\\bobyqa-two\\history",
				"-predictor","lineXs",
				"-x","mapreduce.map.sort.spill.percent,mapred.reduce.tasks",
				"-y","totalTimeCost",
				"-plot_type","line",
				"-plot_title","Tuning summary",
				"-n","3"
		};
		PredictorRunner.main(args);
	}
	
	public static void testExp()  {
		String[] args=new String[] {
				"-dir","E:\\CatlaHS\\bobyqa-two\\history",
				"-predictor","exp",
				"-x","Order",
				"-y","totalTimeCost",
				"-plot_type","line",
				"-plot_title","Tuning summary",
		};
		PredictorRunner.main(args);
	}
	
	public static void testLog()  {
		String[] args=new String[] {
				"-dir","E:\\CatlaHS\\bobyqa-two\\history",
				"-predictor","log",
				"-x","Order",
				"-y","totalTimeCost",
				"-plot_type","line",
				"-plot_title","Tuning summary",
		};
		PredictorRunner.main(args);
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// testPoly();
		// testLine();
		// testLine2();
		// testExp();
		testLog();
	}

}
