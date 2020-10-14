# Catla-HS with Machine Learning

The newly designed Catla-HS support training and predicting the MapReduce performance using machine learning algorithms from various Java based machine learning libraries. We integrate a new scheme in Catla-HS to support such machine learning based applications. 

The key method to support this is retrieve all available job metric data from users' tuning projects and establish the machine learning models based on existing Java machine learning methods. 

Below are the detailed design and examples to support machine learning in Catla-HS. 

## Design

Detailed implementation can be found in the Java package `cn.edu.bjtu.cdh.catla.ml`.

First, we create a Java interface source file to define key methods to implements inside a Machine Learning class in different Java ML libraries. 

The `CatlaML' class is below:

```java
package cn.edu.bjtu.cdh.catla.ml;

public interface CatlaML {
	public double[] learn();

	public void predict();

	public void read(String[] input, String[] output);

	public void set(String key, Object value);
}

```

Then, we create a factory to initialize the use of specific Java ML library. The ```factory``` class is below: 

```java
package cn.edu.bjtu.cdh.catla.ml;

public class CatlaMLFactory {
	
	public static String JavaMachineLearning="java-ml";
	public static String Mahout="mahout";
	
	public static CatlaML createInstance(String lib,String project_folder,String algorithm) {
		if(lib.equals(JavaMachineLearning)) {
			return new CatlaJavaML(project_folder,algorithm);
		}else if (lib.equals(Mahout)){
			return new CatlaMahout();
        
		}
        /// More can be inserted here. 
        else {
			return null;
		}
	}
}

```

Finally, we create an instance of `CatlaML` for the Java ML libraries supported in Catla-HS. 

## Examples

Currently, we implement an instance of the machine learning library ([Java-ML](http://java-ml.sourceforge.net/)) with several common algorithms that can be supported in Catla-HS during the analysis of MapReduce performance. 

Be note that users can always achieve their own libraries following the above design since the project is complete open-source and transparent in implementation. Due to limition of time, we did not implement many libraries. 

Here are three Machine Learning examples currently supported in Catla-HS, which can be seamlessly utilize job metric data during the tuning process. 

### Cluster algorithm

The example use of the cluster algorithm is below:

```java 
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
```

### Feature Selection algorthm 

The feature selection with different ranking/scoring methods is below: 

```java
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
```

### Classification algorithm

The classification algorithm example is below:

```java
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
```

The dataset we used in the above examples can be found [here](example_data). 

## Future 

The future implementation of machine learning methods will include as below:

1. Mahout
2. Spark MLib
3. Deep4j
4. Weka
5. MALLET



