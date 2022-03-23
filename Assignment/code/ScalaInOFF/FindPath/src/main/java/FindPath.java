import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;

import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.apache.spark.graphx.*;
import scala.Serializable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

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

    private static Dataset<Row> getNodeDF(SQLContext sqlContext, String raw_osm_file, String createTempViewName){
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

    private static Dataset<Row> getRoadDF(SQLContext sqlContext, String raw_osm_file, String createTempViewName){
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
        if(!createTempViewName.isEmpty()){
            roadDF.createOrReplaceTempView(createTempViewName);
        } else {
            roadDF.createOrReplaceTempView("roadDF");
        }
        roadDF = sqlContext.sql(
                "select *, checkOneWay(tag) as oneway " +
                        "from roadDF " +
                        "where array_contains(tag._k, \"highway\")");
        if(!createTempViewName.isEmpty()) roadDF.createOrReplaceTempView(createTempViewName);
        return roadDF;
    }

    private static Dataset<Row> getRelationDF(SQLContext sqlContext, String nodeDFViewName, String roadDFViewName, String createTempViewName){

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

    private static String extractStringPath(SQLContext sqlContext, Dataset<Row> paths){
        String edge_vertices = new String();
        for(String col: paths.columns()){
            if(col.contains("v")) edge_vertices += String.format("%s.id, ", col);
        }
        paths.createOrReplaceTempView("paths");
        Row result = sqlContext.sql(
                "select from.id, " +
                        edge_vertices + "to.id " +
                        "from paths"
        ).first();
        String res_string = new String();
        for(int i=0; i<result.size(); i++){
            if(i<result.size()-1){
                res_string+=String.format("%d -> ", result.getLong(i));
            } else {
                res_string+=Long.toString(result.getLong(i));
            }
        }
        return res_string;
    }

    private static String getPathsFromBFS(SQLContext sqlContext, GraphFrame g, String from_node, String to_node){
        from_node = String.format("id = %s", from_node);
        to_node = String.format("id = %s", to_node);
        Dataset<Row> paths = g.bfs().fromExpr(from_node).toExpr(to_node).run();
        return extractStringPath(sqlContext, paths);
    }

    private static Float getRunTime(Long start){
        long end = System.currentTimeMillis();
        return (end - start) / 1000F;
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

    public static void main(String[] args) throws IOException {
        if (!runOnCluster) {
            sparkConf.setMaster("local[2]");
            sparkConf.setJars(new String[] { "target/eduonix_spark-deploy.jar" });
//            spark = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        String raw_osm_file = args[0];
        String input_src_dst_file = args[1];
        String result_adjmap_file = args[2];
        String result_path_file = args[3];

        new GraphFrame();
        long start = System.currentTimeMillis();
        float runtime;

        register_UDFs(sqlContext);

        /* construct relation table */
        Dataset<Row> nodeDF = getNodeDF(sqlContext, raw_osm_file, "nodeDF");
        Dataset<Row> roadDF = getRoadDF(sqlContext, raw_osm_file, "roadDF");
        Dataset<Row> relationDF = getRelationDF(sqlContext, "nodeDF", "roadDF","relationDF").cache();

//        Dataset<Row> adjMapCollection = sqlContext.sql(
//                "select source, concat_ws(\" \", source, sort_array(collect_list(destination),true)) " +
//                        "from relationDF " +
//                        "group by source " );
//        System.out.println(adjMapCollection.where("source=9346078669").collectAsList().toString());

        List<Row> adjMapCollection = sqlContext.sql(
                "select concat_ws(\" \", source, sort_array(collect_list(destination),true)) " +
                        "from relationDF " +
                        "group by source " +
                        "order by source").collectAsList();

//        GraphFrame g = GraphFrame
//                .apply(nodeDF.withColumnRenamed("node_id", "id"),
//                        relationDF.withColumnRenamed("source", "src").withColumnRenamed("destination", "dst"))
//                .cache();;
//
//        List<String> all_results = new ArrayList<String>();
//        Scanner input_tasks = new Scanner(new FileInputStream(input_src_dst_file));
//        while(input_tasks.hasNextLine()){
//            String[] task = input_tasks.nextLine().split(" ");
//            /*BFS*/
//            System.out.println(String.format("Processing task: \nsrc node: %s -> dst node: %s", task[0], task[1]));
//            String result = getPathsFromBFS(sqlContext, g, task[0], task[1]);
//
//            //check run time for task
//            runtime = getRunTime(start);
//            System.out.println(String.format("path found: %s", result));
//            System.out.println(String.format("Runtime: %f seconds", runtime));
//
//            all_results.add(result);
//        }

        jsc.stop();

        FileWriter fw_adjmap = new FileWriter(result_adjmap_file);
        for(Row row: adjMapCollection){
            fw_adjmap.write(row.getString(0)+"\n");
        }
//        FileWriter fw = new FileWriter(result_path_file);
//        for(String res: all_results){
//            fw.write(res+"\n");
//        }
//        fw.close();
//
        runtime = getRunTime(start);
        System.out.println(String.format("Job done overall Runtime: %f seconds", runtime));
    }
}