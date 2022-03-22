import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.apache.spark.graphx.*;
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

    public static Dataset<Row> getNodeDF(SQLContext sqlContext, String raw_osm_file, String createTempViewName){
        Map<String, String> options = new HashMap<>();
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("_lat", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("_lon", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);
        options.put("rowTag", "node");
        Dataset<Row> nodeDF = sqlContext.read()
                .options(options)
                .format("xml")
                .schema(schema)
                .load(raw_osm_file)
                .withColumnRenamed("_id", "node_id")
                .withColumnRenamed("_lat", "latitude")
                .withColumnRenamed("_lon", "longitude").cache();
        if(!createTempViewName.isEmpty()) nodeDF.createOrReplaceTempView(createTempViewName);
        return nodeDF;
    }

    public static Dataset<Row> getRoadDF(SQLContext sqlContext, String raw_osm_file, String createTempViewName){
        Map<String, String> options = new HashMap<>();
        List<StructField> fields = new ArrayList<StructField>(); //reset fields for ways
        fields.add(DataTypes.createStructField("_id", DataTypes.LongType, true));
        List<StructField> inner_fields = new ArrayList<StructField>();
        inner_fields.add(DataTypes.createStructField("_ref", DataTypes.LongType, true));
        ArrayType nd_type = DataTypes.createArrayType(DataTypes.createStructType(inner_fields));
        fields.add(DataTypes.createStructField("nd", nd_type, true));
        inner_fields = new ArrayList<StructField>();
        inner_fields.add(DataTypes.createStructField("_k", DataTypes.StringType, true));
        inner_fields.add(DataTypes.createStructField("_v", DataTypes.StringType, true));
        ArrayType tag_type = DataTypes.createArrayType(DataTypes.createStructType(inner_fields));
        fields.add(DataTypes.createStructField("tag", tag_type, true));
        StructType schema = DataTypes.createStructType(fields);
        options.put("rowTag", "way");
        Dataset<Row> roadDF = sqlContext.read()
                .options(options)
                .format("xml")
                .schema(schema)
                .load(raw_osm_file)
                .withColumnRenamed("_id", "road_id");

        roadDF = roadDF.filter(array_contains(roadDF.col("tag._k"), "highway"))
                .withColumn("oneway", callUDF("checkOneWay", roadDF.col("tag")));

        if(!createTempViewName.isEmpty()) roadDF.createOrReplaceTempView(createTempViewName);
        return roadDF;
    }

    public static Dataset<Row> getRelationDF(SQLContext sqlContext, String nodeDFViewName, String roadDFViewName, String createTempViewName){

        Dataset<Row> expwayNodeDF = sqlContext.sql(
                String.format("select road_id, exploded.nd_index+1 as indexedNode, node._ref as node_id, oneway " +
                        "from %s " +
                        "lateral view posexplode(%s.nd) exploded as nd_index, node", roadDFViewName, roadDFViewName));
        expwayNodeDF.createOrReplaceTempView("expwayNodeDF");
        Dataset<Row> forward_relation = sqlContext.sql(
                String.format("with " +
                        "sourceDF as (" +
                        "select road_id, indexedNode as indexedNode_src, src_node.node_id as source, node_info.latitude as src_latitude, node_info.longitude as src_longitude, oneway " +
                        "from expwayNodeDF as src_node, %s as node_info " +
                        "where src_node.node_id == node_info.node_id), " +
                        "destDF as (" +
                        "select road_id, indexedNode - 1 as indexedNode_dest, dest_node.node_id as destination, node_info.latitude as dest_latitude, node_info.longitude as dest_longitude " +
                        "from expwayNodeDF as dest_node, %s as node_info " +
                        "where dest_node.node_id == node_info.node_id) " +
                        "select s.road_id as road_id, s.source as source, d.destination as destination, computeDistance(s.src_latitude, d.dest_latitude, s.src_longitude, d.dest_longitude) as distance, s.oneway " +
                        "from sourceDF s, destDF d " +
                        "where s.road_id == d.road_id and s.indexedNode_src == d.indexedNode_dest ", nodeDFViewName, nodeDFViewName)).distinct();//computeDistance
        Dataset<Row> reversed_relation = forward_relation.filter("oneway = false")
                .withColumn("rev_src",forward_relation.col("destination"))
                .withColumn("rev_dest", forward_relation.col("source"))
                .drop("destination", "source")
                .withColumnRenamed("rev_src", "source")
                .withColumnRenamed("rev_dest", "destination")
                .select("road_id", "source", "destination", "distance", "oneway");
        Dataset<Row> full_relation = forward_relation.union(reversed_relation);
        if(!createTempViewName.isEmpty()) full_relation.createOrReplaceTempView(createTempViewName);
        return full_relation;
    }

    /* Define & register UDFS */
    private static void register_UDFs(SQLContext sqlContext){
        sqlContext.udf().register("checkOneWay", new UDF1<WrappedArray<Row>, Boolean>(){
            private static final long serialVersionUID = -5372447039252716846L;
            @Override
            public Boolean call(WrappedArray<Row> tags) throws Exception {
               for(int i=0; i<tags.size(); i++){
                   Row tag = tags.apply(i);
                   if(tag.getString(0).equals("oneway") & tag.getString(1).equals("yes")){
                       return true;
                   }
               }
                return false;
            }
        }, DataTypes.BooleanType);

        sqlContext.udf().register("computeDistance", new UDF4<Double, Double, Double, Double, Double>(){
            @Override
            public Double call(Double lat1, Double lat2, Double lon1, Double lon2) throws Exception {
                return distance(lat1, lat2, lon1, lon2);
            }
        }, DataTypes.DoubleType);
    }

    /*Define global variables */
    static boolean runOnCluster = false;
    static SparkConf sparkConf = new SparkConf().setAppName("FindPath");

    public static void main(String[] args) {
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
//            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String raw_osm_file = args[0];
        register_UDFs(sqlContext);

        Dataset<Row> nodeDF = getNodeDF(sqlContext, raw_osm_file, "nodeDF");
        Dataset<Row> roadDF = getRoadDF(sqlContext, raw_osm_file, "roadDF");
        Dataset<Row> relationDF = getRelationDF(sqlContext, "nodeDF", "roadDF","relationDF");
//        sqlContext.sql("select * from relationDF where road_id == 104661187").show(10);
        GraphFrame g = new GraphFrame(nodeDF.withColumnRenamed("node_id", "id"),
                relationDF.withColumnRenamed("source", "src").withColumnRenamed("destination", "dst"));
        /*BFS*/
        String from_node = "id = 1534830314";
        String to_node = "id = 1534822530";
        Dataset<Row> paths = g.bfs().fromExpr(from_node).toExpr(to_node).run();
        paths.show();
//        List inter_edges = paths.first().getList(1);
//        String str_path = "1534830314 ->";
//        for (inter_node: )
//        System.out.println(paths.first().getList(1).toString());
        jsc.stop();
    }
}