package cn.edu.bjtu.cdh.catla.ml;

public interface CatlaML {
	public double[] learn();

	public void predict();

	public void read(String[] input, String[] output);

	public void set(String key, Object value);
}
