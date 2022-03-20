import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;
import org.apache.spark.sql.types.DataTypes;
import org.graphframes.GraphFrame;
import scala.collection.mutable.Seq;
import scala.collection.mutable.WrappedArray;


import java.util.*;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    /*Define global variables */
    static boolean runOnCluster = false;
    static SparkConf sparkConf = new SparkConf().setAppName("FindPath");
//    static SparkSession spark = null;

    public static void main(String[] args) {
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
//            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

//        Register_UDFs(spark);

        Map<String, String> options = new HashMap<>();
        options.put("rowTag", "node");
        Dataset<Row> nodeData = sqlContext.read()
                .options(options)
                .format("xml")
                .load("src/main/resources/NUS.osm");
        nodeData.createOrReplaceTempView("nodeDF");
        Dataset<Row> nodeDF = sqlContext.sql("select _id as node_id, _lat as latitude, _lon as longitude from nodeDF");
        nodeDF.show(10);

        options.put("rowTag", "way");
        Dataset<Row> roadData = sqlContext.read()
                .options(options)
                .format("xml")
                .load("src/main/resources/NUS.osm");

        sqlContext.udf().register("checkOneWay", new UDF1<WrappedArray, Boolean>(){
            private static final long serialVersionUID = -5372447039252716846L;
            @Override
            public Boolean call(WrappedArray tags) throws Exception {
                return tags.toString().contains("oneway");
            }
        }, DataTypes.BooleanType);

        Dataset<Row> highwayDF = roadData.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                return row.getAs("tag").toString().contains("highway");
            }
        }).withColumn("oneway", callUDF("checkOneWay", roadData.col("tag")));

        highwayDF.show(5);
//
//        highwayDF.createOrReplaceTempView("highwayDF");
//        sqlContext.sql("select _id as road_id, nd as nodes, oneway from highwayDF where oneway=true").show(5);

        jsc.stop();
    }
}