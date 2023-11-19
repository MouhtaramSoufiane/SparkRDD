import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AppRDD2 {
    public static void main(String[] args) {
        SparkConf configuration=new SparkConf().setAppName("Ventes").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(configuration);

        JavaRDD<String> RDD = sc.textFile("ventes.txt");

        JavaRDD<String> RDD2 = RDD.map(ligne -> {
            String[] split = ligne.split(" ");
            String ville = Arrays.stream(split).toList().get(1);
            double price = Double.parseDouble(split[3]);
            return ville +" "+ price;
        });

        JavaPairRDD<String, String> rdd = RDD2.mapToPair(s -> new Tuple2<>(Arrays.stream(s.split(" ")).toList().get(0), Arrays.stream(s.split(" ")).toList().get(1)));

        JavaPairRDD<String, String> RDD4 = rdd.reduceByKey((price1, price2) ->
                String.valueOf(Double.parseDouble(price1) + Double.parseDouble(price2))
        );

        RDD4.foreach(s->{
            System.out.println(s);
        });


    }
}
