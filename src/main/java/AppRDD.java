import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppRDD {
    public static void main(String[] args) {
        SparkConf configuration=new SparkConf().setAppName("Etudiants").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(configuration);

        List<String> names = List.of("YASSINE SAWLI", "MOHAMMED YOUSSFI", "SOUFIANE MOUHTARAM", "ABDELJALIL ELMAJJOUDI", "HICHAM EL_MOUDNI");
        JavaRDD<String> RDD1 = sc.parallelize(names);
        JavaRDD<String> RDD2 = RDD1.flatMap(name -> {
            List<String> LastNames = new ArrayList<>();
            String[] strings = name.split(" ");
            String s = Arrays.stream(strings).toList().get(1);
            LastNames.add(s);

            return LastNames.iterator();
        });

        JavaRDD<String> RDD3 = RDD2.filter(name -> {
            return name.length() > 6;
        });

        JavaRDD<String> RDD4 = RDD2.filter(name -> {
            return name.contains("OU");
        });
        JavaRDD<String> RDD5 = RDD2.filter(name -> {
            return name.length()==7;
        });


        JavaRDD<String> RDD6 = RDD3.union(RDD4);

        JavaRDD<String> RDD71 = RDD5.map(m -> {
            return m+71;
        });

        JavaRDD<String> RDD81 = RDD6.map(m -> {
            return m+81;
        });

        JavaPairRDD<String, Integer> RDD7 = RDD71.mapToPair(name ->
                new Tuple2<>(name, 1)
        );

        JavaPairRDD<String, Integer> RDD8 = RDD81.mapToPair(name ->
                new Tuple2<>(name, 1)
        );
        JavaPairRDD<String, Integer> RDD9 = RDD7.union(RDD8);

        RDD9.sortByKey();


    }
}
