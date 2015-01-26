package com.enremmeta.otter.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Seq;
import scala.reflect.ClassTag;

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
	getOpts().addOption("i", true, "Input - HDFS file");
	getOpts().addOption("o", true, "Output - HDFS file");
	getOpts().addOption("c", true, "Cluster count");
	getOpts().addOption("m", true, "Max iterations");
	parseCommandLineArgs(argv);
	this.inFile = getCl().getOptionValue('i');
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
    private String inFile;
    private String outFile;
    public static final String HDFS_PREFIX = "hdfs://ip-10-51-152-144.ec2.internal/user/impala/x5_2/";

    public void runKmeans() {

	SparkConf conf = new SparkConf().setAppName("K-means Example");
	JavaSparkContext sc = new JavaSparkContext(conf);

	// Load and parse data
	String path = HDFS_PREFIX + inFile;
	JavaRDD<String> data = sc.textFile(path);
	JavaRDD<Vector> parsedData = data.map(new ParsingMapper());

	KMeansModel clusters = KMeans.train(
					    parsedData.rdd(),
					    clusterCount,
					    maxIterations);

	// Evaluate clustering by computing Within Set Sum of Squared Errors
	Vector[] centers = clusters.clusterCenters();
	List<Vector> centersList = Arrays.asList(centers);
	Buffer<Vector> buf = JavaConversions.asScalaBuffer(centersList);
	Seq<Vector> seq = buf.seq();
	ClassTag<Vector> tag = scala.reflect.ClassTag$.MODULE$
		.apply(Vector.class);
	RDD<Vector> outRdd = sc.sc().makeRDD(seq, clusterCount, tag);
	String pathOut = HDFS_PREFIX + outFile;
	outRdd.saveAsTextFile(pathOut);

	double WSSSE = clusters.computeCost(parsedData.rdd());
	System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }

    public static void main(String[] args) throws Exception {
	KMeansRunner runner = new KMeansRunner(args);
	runner.runKmeans();

    }
}