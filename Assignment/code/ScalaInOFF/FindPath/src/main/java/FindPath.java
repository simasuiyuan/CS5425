import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Tuple2;
import scala.Tuple3;

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

    static boolean runOnCluster = false;
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("FindPath");
        SparkSession spark = null;
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        } else {
            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        }

        Map<String, String> options = new HashMap<>();
//        options.put("rowTag", "node");
//        Dataset<Row> nodeData = spark.read()
//                .options(options)
//                .format("xml")
//                .load("src/main/resources/NUS.osm");
//        nodeData.createOrReplaceTempView("nodeDF");
//        Dataset<Row> nodeDF = spark.sql("select _id as node_id, _lat as latitude, _lon as longitude from nodeDF");
//        nodeDF.show(10);

//        List<StructField> fields = new ArrayList<>();
//        fields.add(DataTypes.createStructField("_id",DataTypes.StringType, true));
//        StructField nodesStrcut =
//
//        fields.add(DataTypes.createStructField("nd",DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)), true));
////        fields.add(DataTypes.createStructField("tag",DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)), true));
//        StructType schema = DataTypes.createStructType(fields);

        options.put("rowTag", "way");
        Dataset<Row> roadData = spark.read()
                .options(options)
//                .schema(schema)
                .format("xml")
                .load("src/main/resources/NUS.osm");
//        roadData.show(10);
//        roadData.printSchema();
//
//        roadData.createOrReplaceTempView("roadDF");
//        Dataset<Row> highwayDF = spark.sql("select _id as road_id, nd as nodes, tag as tag from roadDF").filter(roadData.select("tag").col("_k").contains("highway"));
//        highwayDF.show();
        roadData.select("tag").flatMap(_._k).show();
        spark.stop();
    }
}
