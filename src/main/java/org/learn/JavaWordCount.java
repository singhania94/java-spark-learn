package org.learn;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile("src//main//resources//textfile.txt").javaRDD();
		String header = lines.first();
		JavaRDD<String> words = lines.filter(s -> !s.equals(header)).flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		JavaPairRDD<String, Integer> fCounts = counts.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
				final Tuple2<String, Integer> t1 = new Tuple2<>(t._1().replaceAll(",", ""), t._2);
				return t1;
			}
		});
		JavaPairRDD<String, Integer> sorted = fCounts.sortByKey(true);

		List<Tuple2<String, Integer>> output = sorted.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		Thread.sleep(100000);
		spark.stop();
	}
}
