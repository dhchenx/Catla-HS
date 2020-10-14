package cn.edu.bjtu.cdh.catla.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import cn.edu.bjtu.cdh.catla.prediction.JobSummary;
import net.sf.javaml.classification.Classifier;
import net.sf.javaml.classification.KNearestNeighbors;
import net.sf.javaml.classification.evaluation.CrossValidation;
import net.sf.javaml.classification.evaluation.PerformanceMeasure;
import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.clustering.evaluation.AICScore;
import net.sf.javaml.clustering.evaluation.BICScore;
import net.sf.javaml.clustering.evaluation.ClusterEvaluation;
import net.sf.javaml.clustering.evaluation.SumOfSquaredErrors;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.DenseInstance;
import net.sf.javaml.core.Instance;
import net.sf.javaml.distance.PearsonCorrelationCoefficient;
import net.sf.javaml.featureselection.ensemble.LinearRankingEnsemble;
import net.sf.javaml.featureselection.ranking.RecursiveFeatureEliminationSVM;
import net.sf.javaml.featureselection.scoring.GainRatio;
import net.sf.javaml.featureselection.subset.GreedyForwardSelection;

public class CatlaJavaML implements CatlaML {

	public static String ML_Cluster = "cluster";
	public static String ML_Feature_Selection = "feature_selection";
	public static String ML_Classification = "classification";

	public static String NUM_TOPIC = "number_of_topic";
	public static String FEATURE_SELECT_METHOD = "feature_select_method";
	public static String KNN_K = "knn_k";

	private String project_folder = null;
	private String alg_type = null;

	private Map<String, Object> parameters = new HashMap<String, Object>();

	public CatlaJavaML(String alg_type, String projectFolder) {
		this.project_folder = projectFolder;
		this.alg_type = alg_type;
	}

	public void set(String key, Object value) {
		parameters.put(key, value);
	}

	@Override
	public double[] learn() {
		// TODO Auto-generated method stub
		if (this.alg_type.equals(ML_Cluster)) {
			return this.learn4Cluster(this.real_dataset, Integer.parseInt(parameters.get(NUM_TOPIC).toString()));
		} else if (this.alg_type.equals(ML_Feature_Selection)) {
			return this.learn4FeatureSelection(this.real_dataset,
					this.parameters.get(FEATURE_SELECT_METHOD).toString());
		} else if (this.alg_type.equals(ML_Classification)) {
			return this.learn4Classifiction(this.real_dataset, Integer.parseInt(this.parameters.get(KNN_K).toString()));
		} else {
			System.err.println("no algorithm found!");
			return null;
		}

	}

	@Override
	public void predict() {
		// TODO Auto-generated method stub

	}

	private Dataset real_dataset = null;

	private Dataset read4Cluster(String[] input, String[] output) {
		JobSummary ts = new JobSummary(this.project_folder);
		String[][][] data = ts.getDataset(input, output);
		String[][] xdata = data[0];
		String[][] ydata = data[1];

		Dataset dataset = new DefaultDataset();
		for (int i = 0; i < xdata.length; i++) {
			/* values of the attributes. */
			double[] values = new double[xdata[i].length];
			for (int j = 0; j < xdata[i].length; j++) {
				values[j] = Double.parseDouble(xdata[i][j]);
			}

			/*
			 * Create and an Instance with the above values and its class label set to the
			 * value from y[0]
			 */
			Instance instance = new DenseInstance(values, ydata[i][0]);

			dataset.add(instance);
		}
		return dataset;
	}

	private Dataset read4FeatureSelection(String[] input, String[] output) {
		JobSummary ts = new JobSummary(this.project_folder);
		String[][][] data = ts.getDataset(input, output);
		String[][] xdata = data[0];
		String[][] ydata = data[1];

		Dataset dataset = new DefaultDataset();
		for (int i = 0; i < xdata.length; i++) {
			/* values of the attributes. */
			double[] values = new double[xdata[i].length];
			for (int j = 0; j < xdata[i].length; j++) {
				values[j] = Double.parseDouble(xdata[i][j]);
			}

			/*
			 * Create and an Instance with the above values and its class label set to the
			 * value from y[0]
			 */
			Instance instance = new DenseInstance(values, ydata[i][0]);

			dataset.add(instance);
		}
		return dataset;
	}

	private List<String> labels = null;

	private Dataset read4Classification(String[] input, String[] output) {
		JobSummary ts = new JobSummary(this.project_folder);
		String[][][] data = ts.getDataset(input, output);
		String[][] xdata = data[0];
		String[][] ydata = data[1];

		Dataset dataset = new DefaultDataset();

		labels = new ArrayList<String>();

		for (int i = 0; i < xdata.length; i++) {
			/* values of the attributes. */
			double[] values = new double[xdata[i].length];
			for (int j = 0; j < xdata[i].length; j++) {
				values[j] = Double.parseDouble(xdata[i][j]);
			}

			Instance instance = new DenseInstance(values, ydata[i][0]);

			if (!labels.contains(ydata[i][0])) {
				labels.add(ydata[i][0]);
			}

			dataset.add(instance);
		}
		return dataset;
	}

	public double[] result_cluster = null;

	private double[] learn4Cluster(Dataset dataset, int topicNum) {

		Clusterer km3 = new KMeans(topicNum);

		Dataset[] clusters = km3.cluster(dataset);

		ClusterEvaluation aic = new AICScore();
		ClusterEvaluation bic = new BICScore();
		ClusterEvaluation sse = new SumOfSquaredErrors();

		double aicScore = aic.score(clusters);
		double bicScore = bic.score(clusters);
		double sseScore = sse.score(clusters);

		// System.out.println("AIC score: " + aicScore);
		// System.out.println("BIC score: " + bicScore);
		// System.out.println("Sum of squared errors: " + sseScore);

		result_cluster = new double[] { aicScore, bicScore, sseScore };
		return this.result_cluster;
	}

	private double[] result_feature_select = null;

	private double[] learn4FeatureSelection(Dataset data, String feature_select_method) {

		double[] rets = null;

		// 1. score
		if (feature_select_method.equals("score")) {
			GainRatio ga = new GainRatio();

			ga.build(data);
			rets = new double[ga.noAttributes()];
			/* Print out the score of each attribute */
			for (int i = 0; i < ga.noAttributes(); i++) {
				// System.out.println(ga.score(i));
				rets[i] = ga.score(i);
			}
		}

		// 2. rank
		if (feature_select_method.equals("rank")) {
			/* Create a feature ranking algorithm */
			RecursiveFeatureEliminationSVM svmrfe = new RecursiveFeatureEliminationSVM(0.2);
			/* Apply the algorithm to the data set */
			svmrfe.build(data);
			/* Print out the rank of each attribute */
			rets = new double[svmrfe.noAttributes()];
			for (int i = 0; i < svmrfe.noAttributes(); i++) {
				// System.out.println(svmrfe.rank(i));
				rets[i] = svmrfe.rank(i);
			}
		}
		// 3. subset
		if (feature_select_method.equals("subset")) {
			GainRatio ga = new GainRatio();
			/* Construct a greedy forward subset selector */
			GreedyForwardSelection gfs = new GreedyForwardSelection(1, new PearsonCorrelationCoefficient());
			/* Apply the algorithm to the data set */
			gfs.build(data);
			/* Print out the attribute that has been selected */
			System.out.println(gfs.selectedAttributes());
			Set<Integer> r = gfs.selectedAttributes();
			rets = new double[r.size()];
			Iterator<Integer> it = r.iterator();
			int cc = 0;
			while (it.hasNext()) {
				Integer str = it.next();
				rets[cc] = str;
				cc++;
			}

		}

		// 4. ensemble
		if (feature_select_method.equals("ensemble")) {
			/* Create a feature ranking algorithm */
			RecursiveFeatureEliminationSVM[] svmrfes = new RecursiveFeatureEliminationSVM[10];

			for (int i = 0; i < svmrfes.length; i++)
				svmrfes[i] = new RecursiveFeatureEliminationSVM(0.2);
			LinearRankingEnsemble ensemble = new LinearRankingEnsemble(svmrfes);
			/* Build the ensemble */
			ensemble.build(data);
			/* Get rank of i-th feature */
			rets = new double[svmrfes.length];
			for (int i = 0; i < svmrfes.length; i++) {
				int rank = ensemble.rank(i);
				rets[i] = rank;
			}

		}

		this.result_feature_select = rets;

		return this.result_feature_select;

	}

	public List<String> getLabels() {
		return this.labels;
	}

	private double[] result_classification_accuracies = null;

	private double[] learn4Classifiction(Dataset data, int k) {

		result_classification_accuracies = new double[this.labels.size()];

		/* Construct KNN classifier */
		Classifier knn = new KNearestNeighbors(k);
		/* Construct new cross validation instance with the KNN classifier */
		CrossValidation cv = new CrossValidation(knn);
		/* Perform 5-fold cross-validation on the data set */
		Map<Object, PerformanceMeasure> p = cv.crossValidation(data);
		for (int i = 0; i < labels.size(); i++) {
			double acc = p.get(labels.get(i)).getAccuracy();
			// System.out.println("Accuracy=" +acc );
			// System.out.println(p);
			result_classification_accuracies[i] = acc;
		}
		return this.result_classification_accuracies;

	}

	@Override
	public void read(String[] input, String[] output) {
		// TODO Auto-generated method stub
		if (this.alg_type.equals(ML_Cluster)) {
			this.real_dataset = read4Cluster(input, output);
		} else if (this.alg_type.equals(ML_Feature_Selection)) {
			this.real_dataset = this.read4FeatureSelection(input, output);
		} else if (this.alg_type.equals(ML_Classification)) {
			this.real_dataset = this.read4Classification(input, output);
		} else {
			System.err.println("no algorithm found!");
		}
	}

	public double[] getLearnResults() {
		if (this.alg_type.equals(ML_Cluster)) {
			return this.result_cluster;
		} else if (this.alg_type.equals(ML_Feature_Selection)) {
			return this.result_feature_select;
		} else if (this.alg_type.equals(ML_Classification)) {
			return this.result_classification_accuracies;
		} else {
			System.err.println("no algorithm found!");
			return null;
		}

	}
 

}
