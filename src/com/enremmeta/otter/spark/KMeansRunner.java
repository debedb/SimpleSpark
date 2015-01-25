package com.enremmeta.otter.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansRunner implements Constants {

    private static final class ParsingMapper implements
	    Function<String, Vector> {

	public Vector call(String s) throws Exception {
	    String[] sarray = s.split(DEFAULT_DELIMITER);
	    double[] values = new double[sarray.length];
	    for (int i = 0; i < sarray.length; i++)
		values[i] = Double.parseDouble(sarray[i]);
	    return Vectors.dense(values);
	}

    };

    public static void main(String[] args) {

	SparkConf conf = new SparkConf().setAppName("K-means Example");
	JavaSparkContext sc = new JavaSparkContext(conf);

	// Load and parse data
	String path = "data/mllib/kmeans_data.txt";
	JavaRDD<String> data = sc.textFile(path);
	JavaRDD<Vector> parsedData = data.map(new ParsingMapper());

	// Cluster the data into two classes using KMeansRunner
	int numClusters = 2;
	int numIterations = 20;
	KMeansModel clusters = KMeans.train(
					    parsedData.rdd(),
					    numClusters,
					    numIterations);

	// Evaluate clustering by computing Within Set Sum of Squared Errors
	double WSSSE = clusters.computeCost(parsedData.rdd());
	System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }
}