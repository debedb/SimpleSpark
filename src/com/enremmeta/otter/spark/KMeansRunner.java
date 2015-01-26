package com.enremmeta.otter.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class KMeansRunner extends ServiceRunner implements Constants {

    private static final class ParsingMapper implements
	    Function<String, Vector> {

	public Vector call(String s) throws Exception {
	    String[] sarray = s.split(DEFAULT_DELIMITER);
	    double[] values = new double[sarray.length];
	    for (int i = 0; i < sarray.length; i++) {
		values[i] = Double.parseDouble(sarray[i]);
	    }
	    return Vectors.dense(values);
	}

    };

    public KMeansRunner(String[] argv) throws Exception {
	super();
	getOpts().addOption("f", true, "HDFS file");
	getOpts().addOption("c", true, "Cluster count");
	getOpts().addOption("m", true, "Max iterations");
	parseCommandLineArgs(argv);
	this.file = getCl().getOptionValue('f');
	String clusterCountStr = getCl().getOptionValue('c');
	if (clusterCountStr == null) {
	    clusterCountStr = "2";
	}
	this.clusterCount = Integer.parseInt(clusterCountStr);
	
	String maxIterationsStr = getCl().getOptionValue('m');
	if (maxIterationsStr == null) {
	    maxIterationsStr = "50";
	}
	this.maxIterations = Integer.parseInt(maxIterationsStr);

    }

    private int clusterCount;
    private int maxIterations;
    private String file;

    public static final String HDFS_PREFIX = "hdfs://ip-10-51-152-144.ec2.internal/user/impala/x5_2/";

    public void runKmeans() {

	SparkConf conf = new SparkConf().setAppName("K-means Example");
	JavaSparkContext sc = new JavaSparkContext(conf);

	// Load and parse data
	String path = HDFS_PREFIX + file;
	JavaRDD<String> data = sc.textFile(path);
	JavaRDD<Vector> parsedData = data.map(new ParsingMapper());

	KMeansModel clusters = KMeans.train(
					    parsedData.rdd(),
					    clusterCount,
					    maxIterations);

	// Evaluate clustering by computing Within Set Sum of Squared Errors
	double WSSSE = clusters.computeCost(parsedData.rdd());
	System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }

    public static void main(String[] args) throws Exception {
	KMeansRunner runner = new KMeansRunner(args);
	runner.runKmeans();

    }
}