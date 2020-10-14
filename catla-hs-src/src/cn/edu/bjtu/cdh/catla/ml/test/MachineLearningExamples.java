package cn.edu.bjtu.cdh.catla.ml.test;

import cn.edu.bjtu.cdh.catla.ml.CatlaJavaML;
import cn.edu.bjtu.cdh.catla.ml.CatlaML;
import cn.edu.bjtu.cdh.catla.ml.CatlaMLFactory;

public class MachineLearningExamples {

	public static void main(String[] args) {
		testClusterAlgorithm();
		testFeatureSelectionAlgorithm();
		testClassificationAlgorithm();
	}

	// A clustering algorithm creates a division of the orginal dataset.
	public static void testClusterAlgorithm() {

		CatlaML cml = CatlaMLFactory.createInstance(CatlaMLFactory.JavaMachineLearning, "E:\\CatlaHS\\cdh_mr_reducejoin",CatlaJavaML.ML_Cluster);

		cml.set(CatlaJavaML.NUM_TOPIC, 3);

		cml.read(new String[] { "totalTimeCost", "jobTimeCost" }, new String[] { "mapred.reduce.tasks" });

		double[] results = cml.learn();

		System.out.println("AIC score: " + results[0]);
		System.out.println("BIC score: " + results[1]);
		System.out.println("Sum of squared errors: " + results[2]);

	}

	//All feature scoring algorithms implements the following method. 
	public static void testFeatureSelectionAlgorithm() {

		CatlaML cml = CatlaMLFactory.createInstance(CatlaMLFactory.JavaMachineLearning, "E:\\CatlaHS\\cdh_mr_reducejoin",CatlaJavaML.ML_Feature_Selection);

		cml.set(CatlaJavaML.FEATURE_SELECT_METHOD, "score");// you can change to other values for different selection
															// methods, namely, score, rank, subset, ensemble

		String[] x_fields = new String[] { "totalTimeCost", "jobTimeCost" };

		cml.read(x_fields, new String[] { "mapred.reduce.tasks" });

		double[] results = cml.learn();

		// print out each score of attribute
		System.out.println("------result------");
		for (int i = 0; i < x_fields.length; i++) {
			System.out.println(x_fields[i] + "'s score is: " + results[i]);
		}

	}

	// the basics of setting up a classifier, training the algorithm and evaluating its performance.
	public static void testClassificationAlgorithm() {
		CatlaML cml = CatlaMLFactory.createInstance(CatlaMLFactory.JavaMachineLearning, "E:\\CatlaHS\\cdh_mr_reducejoin",CatlaJavaML.ML_Classification);

		cml.set(CatlaJavaML.KNN_K, "5");

		cml.read(new String[] { "totalTimeCost", "jobTimeCost" }, new String[] { "mapred.reduce.tasks" });

		double[] results = cml.learn();

		System.out.println("------result-------");
		for (int i = 0; i <( (CatlaJavaML)cml).getLabels().size(); i++) {
			System.out.println("Label " + ((CatlaJavaML)cml).getLabels().get(i) + "'s accuracy: " + results[i]);
		}

	}

}
