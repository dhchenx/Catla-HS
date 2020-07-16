package cn.edu.bjtu.cdh.catla.optimization;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.analysis.MultivariateFunction;

import cn.edu.bjtu.cdh.catla.task.HadoopEnv;
import cn.edu.bjtu.cdh.catla.task.HadoopProject;
import cn.edu.bjtu.cdh.catla.tuning.HadoopTuning;
import cn.edu.bjtu.cdh.catla.utils.CatlaFileUtils;

public class HT_GridSearch implements ITaskOptimizer {

	private Map<String, String> options;

	public HT_GridSearch(Map<String, String> options) {
		this.options = options;
	}

	private double innerCost;

	public static HadoopEnv MapToEnv(Map<String, String> map) {
		HadoopEnv he = new HadoopEnv();
		he.setMasterHost(map.get("MasterHost"));
		he.setMasterPassword(map.get("MasterPassword"));
		he.setMasterPort(Integer.parseInt(map.get("MasterPort")));
		he.setMasterUser(map.get("MasterUser"));
		he.setHadoopBin(map.get("HadoopBin"));
		he.setAppRoot(map.get("AppRoot"));
		if(map.keySet().contains("SparkUrl"))
		{
			he.setSparkUrl(map.get("SparkUrl"));
		}
		return he;
	}
	
	@Override
	public double[] optimize(MultivariateFunction func, int maxInteration, double[] initValues, double[] lowerBounds,
			double[] upperBounds) {
		// TODO Auto-generated method stub
		String dirFolder = options.get("-dir");

		HadoopTuning htuning = new HadoopTuning(dirFolder);
		htuning.loadParameters();

		String quickTuningCofigPath = dirFolder + "/tuning/current.txt";
		String[] useParameters = new String[] {};

		if (options.containsKey("-params")) {
			useParameters = options.get("-params").split(";");
		} else {
			System.out.println("using param settings from tuning/current.txt");
			if (new File(quickTuningCofigPath).exists()) {
				List<String> lines = CatlaFileUtils.readFileByLine(quickTuningCofigPath);
				useParameters = new String[lines.size()];
				for (int i = 0; i < lines.size(); i++)
					useParameters[i] = lines.get(i);
			}
		}
		HadoopProject hp1 = HadoopProject.createInstance(new
				  File(dirFolder).getAbsolutePath());
		
		HadoopEnv he=MapToEnv(hp1.getEnvMaps().get(0));
		//support spark
		String appType=he.getAppType();
		if(appType.equals("Spark")) {
			htuning.createSparkOtherArgPairs(useParameters);
		}else
		if(appType.equals("Hadoop")) {
			htuning.createHadoopOtherArgPairs(useParameters);
		}else {
			System.out.println("No valid platform name!");
		}
		
		

		double bestSolution = Double.MAX_VALUE;
		double[] bestV = null;
		for (int i = 0; i < htuning.getOtherArgLists().size(); i++) {

			String otherArgs = htuning.getOtherArgLists().get(i);
			System.out.println("other args: " + otherArgs);
			Map<String,String> currentParameters=null;
			
			if(appType.equals("Hadoop")) {
				currentParameters=htuning.obtainHadoopJobArgs(otherArgs);
			}else if (appType.equals("Spark")) {
				currentParameters=htuning.obtainSparkJobArgs(otherArgs);
			}

			double[] v = new double[useParameters.length];
			for (int j = 0; j < useParameters.length; j++) {
				System.out.println("key: "+useParameters[j]);
				System.out.println("value:"+currentParameters.get(useParameters[j]));
				v[j] = Double.parseDouble(currentParameters.get(useParameters[j]));
			}

			double score = func.value(v);

			if (i == 0) {
				bestV = v;
				bestSolution = score;
			} else {
				if (score < bestSolution) {
					bestV = v;
					bestSolution = score;
				}

			}
			this.innerCost = bestSolution;
			System.out.println("Iteration: " + (i + 1) + " Best solution: " + bestSolution);

		}

		return bestV;
	}

	@Override
	public double getCost() {
		// TODO Auto-generated method stub

		return innerCost;
	}

}
