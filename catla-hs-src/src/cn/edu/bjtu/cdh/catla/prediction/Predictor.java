package cn.edu.bjtu.cdh.catla.prediction;

public interface Predictor {
	public void fit();
	
	public void predict();
	
	public void plot();
	
	public void plotError();
		
}
